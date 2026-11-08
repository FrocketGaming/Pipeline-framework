"""Command line entry point: ``pipeline list | validate | plan | run``."""

from __future__ import annotations

import os
import sys
import threading
from pathlib import Path

import typer

from .manifest import Job, JobError, discover, load_job
from .runner import JobReport, JobRunner, StepResult, format_bytes, run_jobs

DEFAULT_JOBS_DIR = os.environ.get("PIPELINE_JOBS_DIR", "jobs")
_PRINT_LOCK = threading.Lock()

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Run SQL jobs against BigQuery.",
)


@app.callback()
def _configure(
    ctx: typer.Context,
    jobs_dir: Path = typer.Option(
        Path(DEFAULT_JOBS_DIR), "--jobs-dir", help="directory holding jobs."
    ),
) -> None:
    """Options shared by every command."""
    ctx.obj = jobs_dir


def _fail(message: str, code: int = 2) -> typer.Exit:
    typer.secho(f"error: {message}", fg=typer.colors.RED, err=True)
    return typer.Exit(code=code)


def _load(name: str, jobs_dir: Path) -> Job:
    try:
        return load_job(name, jobs_dir)
    except JobError as exc:
        raise _fail(str(exc)) from exc


def _parse_vars(pairs: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise JobError(f"--var expects k=v, got {pair!r}")
        key, value = pair.split("=", 1)
        values[key.strip()] = value
    return values


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
        print(f"  {report.job}: {status} ({report.progress} steps)", flush=True)
        if not report.ok:
            print(f"    {report.error}", flush=True)


@app.command("list")
def list_jobs(ctx: typer.Context) -> None:
    """List jobs and their step counts."""
    for name, path in sorted(discover(ctx.obj).items()):
        try:
            job = Job.load(path)
        except JobError as exc:
            typer.echo(f"{name:<24} INVALID  {exc}")
            continue
        typer.echo(f"{name:<24} {len(job.steps):>2} steps  dataset={job.dataset or '-'}")


@app.command()
def validate(
    ctx: typer.Context,
    job: list[str] | None = typer.Argument(None, help="Jobs to check (default: all)."),
) -> None:
    """Check every job.toml without touching BigQuery."""
    names = job or sorted(discover(ctx.obj))
    failed = 0
    for name in names:
        try:
            loaded = load_job(name, ctx.obj)
        except JobError as exc:
            failed += 1
            typer.echo(f"{name}: INVALID - {exc}")
            continue
        typer.echo(f"{name}: ok ({len(loaded.steps)} steps, {len(loaded.levels())} levels)")
    if failed:
        raise typer.Exit(code=1)


@app.command()
def plan(ctx: typer.Context, job: str = typer.Argument(..., help="Job to inspect.")) -> None:
    """Show a job's execution levels."""
    loaded = _load(job, ctx.obj)
    typer.echo(f"{loaded.name}  dataset={loaded.dataset or '-'}")
    for depth, level in enumerate(loaded.levels(), start=1):
        typer.echo(f"  level {depth}")
        for step in level:
            depends = f"  <- {', '.join(step.depends_on)}" if step.depends_on else ""
            typer.echo(f"    {step.name:<16} {step.file.name}{depends}")


@app.command()
def run(
    ctx: typer.Context,
    job: list[str] | None = typer.Option(
        None, "--job", "--job-name", "-j", help="Job to run (repeatable)."
    ),
    all_jobs: bool = typer.Option(False, "--all", help="Run every job."),
    step: list[str] | None = typer.Option(
        None, "--step", "-s", help="Run only these steps of one job (repeatable)."
    ),
    with_deps: bool = typer.Option(
        False, "--with-deps", help="With --step, also run the steps they depend on."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Validate every step and report bytes; write nothing."
    ),
    project: str = typer.Option("", "--project", help="GCP project (default: ADC project)."),
    dataset: str = typer.Option("", "--dataset", help="Override every job's dataset."),
    var: list[str] | None = typer.Option(
        None, "--var", help="Query parameter as k=v (repeatable)."
    ),
    max_jobs: int = typer.Option(4, "--max-jobs", help="Jobs running at once."),
    max_workers: int | None = typer.Option(None, "--max-workers", help="Parallel steps per job."),
    max_concurrency: int = typer.Option(
        8, "--max-concurrency", help="Cap on in-flight BigQuery jobs."
    ),
    status_table: str = typer.Option(
        "", "--status-table", help="Append one zbq status row per step here."
    ),
) -> None:
    """Run one or more jobs, or just a few of their steps."""
    names = sorted(set(job or []))
    if all_jobs:
        names = sorted(discover(ctx.obj))
    if not names:
        raise _fail("pass --job NAME (repeatable) or --all")
    if step and len(names) != 1:
        raise _fail("--step requires exactly one --job")

    jobs = [_load(name, ctx.obj) for name in names]
    if step:
        try:
            jobs = [jobs[0].subset(step, with_dependencies=with_deps)]
        except JobError as exc:
            raise _fail(str(exc)) from exc

    try:
        parameters = _parse_vars(var or [])
    except JobError as exc:
        raise _fail(str(exc)) from exc

    from zbq import BigQueryHandler

    client = BigQueryHandler(project_id=project, status_table=status_table or None)
    project = project or client.project_id
    if not project:
        raise _fail("no project; pass --project or set GOOGLE_CLOUD_PROJECT")

    if step:
        typer.echo(f"running {len(jobs[0].steps)} step(s) of {jobs[0].name}", err=False)

    runner = JobRunner(client, max_concurrency=max_concurrency, dry_run=dry_run)
    mode = "dry run" if dry_run else f"{len(jobs)} job(s)"
    print(
        f"{mode}  project={project}  dataset={dataset or 'per job'}",
        flush=True,
    )

    batch = run_jobs(
        jobs,
        runner,
        project=project,
        dataset=dataset,
        parameters=parameters,
        max_jobs=max_jobs,
        max_workers=max_workers,
        on_step=_print_step,
        on_job=_print_job,
    )

    typer.echo()
    typer.echo(batch.summary())
    if not batch.ok:
        raise typer.Exit(code=1)


def main(argv: list[str] | None = None) -> int:
    """Programmatic entry point; prefer the ``pipeline`` console script."""
    try:
        app(argv, standalone_mode=True)
    except SystemExit as exc:  # typer/click exits through SystemExit
        return int(exc.code or 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
