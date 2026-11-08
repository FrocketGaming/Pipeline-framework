"""Execution: run one job's levels, or several jobs at once.

The runner is deliberately decoupled from Google Cloud. It only needs an object
with an ``execute(...)`` method (``zbq.BigQueryHandler`` provides one), which
keeps every branch below testable without credentials.

Each step is one ``client.execute()`` call — never ``read()`` — so no result
rows are ever pulled into this process.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from .manifest import Job, JobError

__all__ = [
    "BatchReport",
    "JobReport",
    "JobRunner",
    "StepFailed",
    "StepResult",
    "format_bytes",
    "run_job",
    "run_jobs",
]


class StepFailed(JobError):
    """A step failed; the rest of that level and all later levels were skipped."""

    def __init__(
        self, job: str, step: str, cause: BaseException, results: Mapping[str, StepResult]
    ):
        super().__init__(f"{job}: step '{step}' failed: {cause}")
        self.job = job
        self.step = step
        self.cause = cause
        self.results = dict(results)
        self.report: JobReport | None = None


def format_bytes(count: int) -> str:
    size = float(count)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TiB"


@dataclass
class StepResult:
    job: str
    step: str
    state: str
    job_id: str | None
    statement_type: str | None
    bytes_processed: int
    rows: int | None
    duration: float


@dataclass
class JobReport:
    job: str
    run_id: str
    results: list[StepResult] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def total_bytes(self) -> int:
        return sum(result.bytes_processed for result in self.results)

    @property
    def total_duration(self) -> float:
        return sum(result.duration for result in self.results)

    def summary(self) -> str:
        lines = [f"{self.job}  run {self.run_id}"]
        for result in self.results:
            detail = result.statement_type or ""
            lines.append(
                f"  {result.step:<16} {result.state:<8} "
                f"{format_bytes(result.bytes_processed):>10} "
                f"{result.duration:>6.1f}s  {result.job_id or ''}  {detail}"
            )
        if self.error:
            lines.append(f"  ERROR: {self.error}")
        return "\n".join(lines)


@dataclass
class BatchReport:
    reports: list[JobReport] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(report.ok for report in self.reports)

    @property
    def failed(self) -> list[str]:
        return [report.job for report in self.reports if not report.ok]

    def summary(self) -> str:
        width = max([len(report.job) for report in self.reports] + [3])
        lines = [f"{'job'.ljust(width)}  {'status':<6}  {'steps':>5}  {'scanned':>10}  {'sec':>6}"]
        for report in self.reports:
            lines.append(
                f"{report.job.ljust(width)}  "
                f"{'ok' if report.ok else 'FAILED':<6}  "
                f"{len(report.results):>5}  "
                f"{format_bytes(report.total_bytes):>10}  "
                f"{report.total_duration:>6.1f}"
            )
        succeeded = sum(1 for report in self.reports if report.ok)
        lines.append("")
        lines.append(f"{succeeded}/{len(self.reports)} jobs succeeded")
        return "\n".join(lines)


class JobRunner:
    """Shared execution backend: caps concurrent BigQuery jobs across all jobs.

    ``zbq`` clients are cheap to reuse and safe across threads; the semaphore is
    what stops N jobs x M parallel steps from bursting into the concurrent-job
    quota.
    """

    def __init__(self, client: Any, *, max_concurrency: int = 8):
        self._client = client
        self._semaphore = threading.BoundedSemaphore(max(1, max_concurrency))

    def execute(
        self,
        sql: str,
        *,
        timeout: int | None = None,
        parameters: dict[str, Any] | None = None,
        max_bytes_billed: int | None = None,
        job_name: str | None = None,
    ):
        with self._semaphore:
            return self._client.execute(
                sql,
                timeout=timeout,
                parameters=dict(parameters or {}),
                max_bytes_billed=max_bytes_billed,
                job_name=job_name,
            )


def run_job(
    job: Job,
    runner: Any,
    *,
    project: str,
    dataset: str = "",
    parameters: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    max_workers: int | None = None,
    on_step: Callable[[StepResult], None] | None = None,
) -> JobReport:
    """Run one job, level by level. Raises StepFailed on the first failing step."""
    context = {"project": project, "dataset": dataset or job.dataset}
    if not context["dataset"]:
        raise JobError(
            f"job '{job.name}' has no dataset; set 'dataset' in job.toml or pass --dataset"
        )

    run_id = run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    report = JobReport(job=job.name, run_id=run_id)

    def run_step(step) -> StepResult:
        started = time.perf_counter()
        result = runner.execute(
            step.sql(context),
            timeout=step.timeout,
            parameters=parameters or {},
            max_bytes_billed=step.max_bytes,
            # Recorded as the status-table label, when one is configured.
            job_name=f"{job.name}.{step.name}",
        )
        record = StepResult(
            job=job.name,
            step=step.name,
            state="DONE",
            job_id=result.job_id,
            statement_type=result.statement_type,
            bytes_processed=result.total_bytes_processed or 0,
            rows=result.num_dml_affected_rows,
            duration=time.perf_counter() - started,
        )
        if on_step is not None:
            on_step(record)
        return record

    for level in job.levels():
        with ThreadPoolExecutor(max_workers=max_workers or job.max_workers) as pool:
            futures = {pool.submit(run_step, step): step for step in level}
            for future in as_completed(futures):
                step = futures[future]
                try:
                    report.results.append(future.result())
                except Exception as exc:
                    # Drop queued siblings and never start the next level. Steps
                    # already running are left to finish: zbq's execute() is
                    # blocking and exposes no cancel handle.
                    for pending in futures:
                        pending.cancel()
                    report.error = f"step '{step.name}' failed: {exc}"
                    report.results.append(
                        StepResult(
                            job=job.name,
                            step=step.name,
                            state="FAILED",
                            job_id=getattr(exc, "job_id", None),
                            statement_type=None,
                            bytes_processed=getattr(exc, "bytes_processed", 0),
                            rows=None,
                            duration=0.0,
                        )
                    )
                    failure = StepFailed(
                        job.name,
                        step.name,
                        exc,
                        {result.step: result for result in report.results},
                    )
                    failure.report = report
                    raise failure from exc

    return report


def run_jobs(
    jobs: list[Job] | tuple[Job, ...],
    runner: Any,
    *,
    project: str,
    dataset: str = "",
    parameters: Mapping[str, Any] | None = None,
    max_jobs: int = 4,
    max_workers: int | None = None,
    on_step: Callable[[StepResult], None] | None = None,
    on_job: Callable[[JobReport], None] | None = None,
) -> BatchReport:
    """Run several jobs concurrently.

    One job failing never stops the others: each gets its own report, and the
    batch is ``ok`` only if every job succeeded.
    """
    batch = BatchReport()
    if not jobs:
        return batch

    with ThreadPoolExecutor(max_workers=max(1, min(max_jobs, len(jobs)))) as pool:
        futures = {
            pool.submit(
                run_job,
                job,
                runner,
                project=project,
                dataset=dataset,
                parameters=parameters,
                max_workers=max_workers,
                on_step=on_step,
            ): job
            for job in jobs
        }
        for future in as_completed(futures):
            job = futures[future]
            try:
                report = future.result()
            except StepFailed as exc:
                report = exc.report or JobReport(job=job.name, run_id="-", error=str(exc))
            except Exception as exc:  # noqa: BLE001 - one job must not abort the batch
                report = JobReport(job=job.name, run_id="-", error=f"{type(exc).__name__}: {exc}")
            batch.reports.append(report)
            if on_job is not None:
                on_job(report)

    batch.reports.sort(key=lambda report: report.job)
    return batch
