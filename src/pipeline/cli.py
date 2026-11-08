"""Command line entry point: ``pipeline list | validate | plan | run``."""

from __future__ import annotations

import argparse
import os
import sys
import threading

from .manifest import Job, JobError, discover, load_job
from .runner import JobReport, JobRunner, StepResult, format_bytes, run_jobs

DEFAULT_JOBS_DIR = os.environ.get("PIPELINE_JOBS_DIR", "jobs")
_PRINT_LOCK = threading.Lock()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description="Run SQL jobs against BigQuery.")
    parser.add_argument(
        "--jobs-dir", default=DEFAULT_JOBS_DIR, help="directory holding jobs (default: jobs)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list jobs")

    validate = sub.add_parser("validate", help="check manifests without touching BigQuery")
    validate.add_argument("job", nargs="*")

    plan = sub.add_parser("plan", help="show a job's execution levels")
    plan.add_argument("job")

    run = sub.add_parser("run", help="run one or more jobs")
    run.add_argument(
        "--job",
        "--job_name",
        dest="job",
        action="append",
        default=[],
        metavar="NAME",
        help="job to run (repeatable)",
    )
    run.add_argument("--all", action="store_true", help="run every job")
    run.add_argument("--project", default="", help="GCP project (default: the ADC project)")
    run.add_argument("--dataset", default="", help="override every job's dataset")
    run.add_argument(
        "--var",
        action="append",
        default=[],
        metavar="k=v",
        help="query parameter shared by every step (repeatable)",
    )
    run.add_argument("--max-jobs", type=int, default=4, help="jobs at once (default 4)")
    run.add_argument("--max-workers", type=int, default=None, help="parallel steps per job")
    run.add_argument(
        "--max-concurrency",
        type=int,
        default=8,
        help="cap on in-flight BigQuery jobs (default 8)",
    )
    run.add_argument(
        "--status-table",
        default="",
        help="append one zbq status row per step here, e.g. proj.dataset.runs",
    )
    return parser


def _print_step(result: StepResult) -> None:
    with _PRINT_LOCK:
        print(
            f"  {result.job}/{result.step:<14} {result.state:<8} "
            f"{format_bytes(result.bytes_processed):>10} {result.duration:>6.1f}s",
            flush=True,
        )


def _print_job(report: JobReport) -> None:
    with _PRINT_LOCK:
        status = "ok" if report.ok else "FAILED"
        print(f"  {report.job}: {status} ({len(report.results)} steps)", flush=True)


def _list(args: argparse.Namespace) -> int:
    for name, path in sorted(discover(args.jobs_dir).items()):
        try:
            job = Job.load(path)
            print(f"{name:<24} {len(job.steps):>2} steps  dataset={job.dataset or '-'}")
        except JobError as exc:
            print(f"{name:<24} INVALID  {exc}")
    return 0


def _validate(args: argparse.Namespace) -> int:
    names = args.job or sorted(discover(args.jobs_dir))
    failed = 0
    for name in names:
        try:
            job = load_job(name, args.jobs_dir)
            print(f"{name}: ok ({len(job.steps)} steps, {len(job.levels())} levels)")
        except JobError as exc:
            failed += 1
            print(f"{name}: INVALID - {exc}")
    return 1 if failed else 0


def _plan(args: argparse.Namespace) -> int:
    job = load_job(args.job, args.jobs_dir)
    print(f"{job.name}  dataset={job.dataset or '-'}")
    for depth, level in enumerate(job.levels(), start=1):
        print(f"  level {depth}")
        for step in level:
            depends = f"  <- {', '.join(step.depends_on)}" if step.depends_on else ""
            print(f"    {step.name:<16} {step.file.name}{depends}")
    return 0


def _parse_vars(pairs: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise JobError(f"--var expects k=v, got {pair!r}")
        key, value = pair.split("=", 1)
        values[key.strip()] = value
    return values


def _run(args: argparse.Namespace) -> int:
    names = sorted(set(args.job))
    if args.all:
        names = sorted(discover(args.jobs_dir))
    if not names:
        print("error: pass --job NAME (repeatable) or --all", file=sys.stderr)
        return 2

    # Validate every manifest before touching BigQuery.
    jobs = [load_job(name, args.jobs_dir) for name in names]
    parameters = _parse_vars(args.var)

    from zbq import BigQueryHandler

    client = BigQueryHandler(
        project_id=args.project,
        status_table=args.status_table or None,
    )
    project = args.project or client.project_id
    if not project:
        print("error: no project; pass --project or set GOOGLE_CLOUD_PROJECT", file=sys.stderr)
        return 2

    runner = JobRunner(client, max_concurrency=args.max_concurrency)
    print(
        f"{len(jobs)} job(s)  project={project}  dataset={args.dataset or 'per job'}",
        flush=True,
    )

    batch = run_jobs(
        jobs,
        runner,
        project=project,
        dataset=args.dataset,
        parameters=parameters,
        max_jobs=args.max_jobs,
        max_workers=args.max_workers,
        on_step=_print_step,
        on_job=_print_job,
    )

    print()
    print(batch.summary())
    return 0 if batch.ok else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "list":
            return _list(args)
        if args.command == "validate":
            return _validate(args)
        if args.command == "plan":
            return _plan(args)
        return _run(args)
    except JobError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
