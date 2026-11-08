"""Job definitions.

A job is a directory::

    jobs/daily_orders/
        job.toml
        queries/01_stg_orders.sql

``job.toml`` names the steps and their dependencies; each SQL file holds one
statement. ``project`` and ``dataset`` are the job's default targets, substituted
into ``{{ project }}`` / ``{{ dataset }}``; the CLI flags of the same name
override them per run. Nothing here imports Google Cloud, so jobs can be listed,
validated and planned without credentials.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "Job",
    "JobError",
    "ManifestError",
    "Step",
    "discover",
    "load_job",
    "parse_bytes",
    "render",
]


class JobError(Exception):
    """Base error for job definitions and execution."""


class ManifestError(JobError):
    """job.toml or one of its SQL files is invalid."""


_PLACEHOLDER = re.compile(r"\{\{\s*([a-z_]+)\s*\}\}")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_BYTES = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*$")
_UNITS = {
    "": 1,
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
}

_STEP_KEYS = {"name", "file", "depends_on", "timeout", "max_bytes"}
_JOB_KEYS = {"project", "dataset", "max_workers", "step"}


def parse_bytes(value: int | str) -> int:
    """Parse ``"50GiB"`` / ``"1GB"`` / ``1048576`` into a byte count."""
    if isinstance(value, bool):
        raise ManifestError(f"not a byte size: {value!r}")
    if isinstance(value, int):
        return value
    match = _BYTES.match(str(value))
    if not match:
        raise ManifestError(f"cannot parse byte size: {value!r}")
    unit = match.group(2).upper()
    if unit not in _UNITS:
        raise ManifestError(f"unknown byte unit {unit!r} in {value!r}")
    return int(float(match.group(1)) * _UNITS[unit])


def render(sql: str, context: Mapping[str, str]) -> str:
    """Substitute ``{{ project }}`` / ``{{ dataset }}`` with validated identifiers.

    Only identifier literals are substituted. Data values belong in query
    parameters (``@run_date``), never in the SQL string.
    """

    def substitute(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in context:
            raise ManifestError(f"unresolved placeholder '{{{{ {key} }}}}' in SQL")
        value = str(context[key])
        if not _IDENTIFIER.match(value):
            raise ManifestError(f"invalid identifier for '{{{{ {key} }}}}': {value!r}")
        return value

    return _PLACEHOLDER.sub(substitute, sql)


@dataclass(frozen=True)
class Step:
    """One SQL file plus its dependencies and guards."""

    name: str
    file: Path
    depends_on: tuple[str, ...] = ()
    timeout: int | None = None
    max_bytes: int | None = None

    def sql(self, context: Mapping[str, str]) -> str:
        try:
            text = self.file.read_text(encoding="utf-8")
        except OSError as exc:
            raise ManifestError(f"{self.file.name}: {exc}") from exc
        return render(text, context)


@dataclass
class Job:
    """A validated job: an ordered DAG of SQL steps."""

    name: str
    root: Path
    steps: tuple[Step, ...]
    project: str = ""
    dataset: str = ""
    max_workers: int = 4

    # -- construction ------------------------------------------------------- #
    @classmethod
    def load(cls, path: str | Path) -> Job:
        """Load and validate ``job.toml``."""
        job_path = Path(path).resolve()
        try:
            raw = tomllib.loads(job_path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ManifestError(f"job.toml not found: {job_path}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ManifestError(f"{job_path.name}: {exc}") from exc

        unknown = set(raw) - _JOB_KEYS
        if unknown:
            raise ManifestError(f"{job_path.name}: unknown key(s) {sorted(unknown)}")
        if not raw.get("step"):
            raise ManifestError(f"{job_path.name}: no [[step]] entries")
        if not isinstance(raw.get("step"), list):
            raise ManifestError(f"{job_path.name}: 'step' must be an array of tables")

        for key in ("project", "dataset"):
            if not isinstance(raw.get(key, ""), str):
                raise ManifestError(f"{job_path.name}: '{key}' must be a string")

        root = job_path.parent
        steps: list[Step] = []
        for index, entry in enumerate(raw["step"], start=1):
            unknown = set(entry) - _STEP_KEYS
            if unknown:
                raise ManifestError(
                    f"{job_path.name} step #{index}: unknown key(s) {sorted(unknown)}"
                )
            name, filename = entry.get("name"), entry.get("file")
            if not name or not filename:
                raise ManifestError(
                    f"{job_path.name} step #{index}: 'name' and 'file' are required"
                )

            depends_on = entry.get("depends_on", [])
            if isinstance(depends_on, str):
                raise ManifestError(
                    f"{job_path.name} step #{index}: 'depends_on' must be a list of step "
                    f"names, not a string (got {depends_on!r})"
                )
            if not isinstance(depends_on, list) or not all(
                isinstance(dep, str) for dep in depends_on
            ):
                raise ManifestError(
                    f"{job_path.name} step #{index}: 'depends_on' must be a list of strings"
                )

            timeout = entry.get("timeout")
            if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int)):
                raise ManifestError(f"{job_path.name} step #{index}: 'timeout' must be an integer")

            steps.append(
                Step(
                    name=str(name),
                    file=root / str(filename),
                    depends_on=tuple(depends_on),
                    timeout=timeout,
                    max_bytes=(parse_bytes(entry["max_bytes"]) if "max_bytes" in entry else None),
                )
            )

        job = cls(
            name=root.name,
            root=root,
            steps=tuple(steps),
            project=raw.get("project", ""),
            dataset=raw.get("dataset", ""),
            max_workers=max(1, int(raw.get("max_workers", 4))),
        )
        job._validate()
        return job

    def _validate(self) -> None:
        errors: list[str] = []
        seen: set[str] = set()
        for step in self.steps:
            if step.name in seen:
                errors.append(f"duplicate step name '{step.name}'")
            seen.add(step.name)

        for step in self.steps:
            for dependency in step.depends_on:
                if dependency == step.name:
                    errors.append(f"step '{step.name}' depends on itself")
                elif dependency not in seen:
                    errors.append(f"step '{step.name}' depends on unknown step '{dependency}'")
            if not step.file.is_file():
                errors.append(f"step '{step.name}': missing file {step.file.name}")

        if not errors:
            try:
                self.levels()
            except ManifestError as exc:
                errors.append(str(exc))

        if errors:
            raise ManifestError("; ".join(errors))

    # -- navigation --------------------------------------------------------- #
    def levels(self) -> list[list[Step]]:
        """Group steps into dependency levels, preserving declaration order.

        Steps in one level run in parallel; every level waits for the previous.
        """
        pending = {step.name: step for step in self.steps}
        done: set[str] = set()
        levels: list[list[Step]] = []

        while pending:
            ready = [
                step
                for step in pending.values()
                if all(dependency in done for dependency in step.depends_on)
            ]
            if not ready:
                raise ManifestError(f"dependency cycle among: {sorted(pending)}")
            levels.append(ready)
            for step in ready:
                done.add(step.name)
                pending.pop(step.name)
        return levels

    def subset(self, names: list[str] | tuple[str, ...], *, with_dependencies: bool = False) -> Job:
        """Copy of this job containing only *names*.

        Dependencies outside the selection are dropped, so a subset of steps
        runs as its own small DAG — which is what makes single-step testing
        work. With ``with_dependencies`` the ancestors come along.
        """
        known = {step.name: step for step in self.steps}
        unknown = [name for name in names if name not in known]
        if unknown:
            raise ManifestError(f"unknown step(s): {', '.join(unknown)} (have: {', '.join(known)})")

        selected = set(names)
        if with_dependencies:
            pending = list(selected)
            while pending:
                for dependency in known[pending.pop()].depends_on:
                    if dependency not in selected:
                        selected.add(dependency)
                        pending.append(dependency)

        steps = [
            Step(
                name=step.name,
                file=step.file,
                depends_on=tuple(d for d in step.depends_on if d in selected),
                timeout=step.timeout,
                max_bytes=step.max_bytes,
            )
            for step in self.steps
            if step.name in selected
        ]
        return Job(
            name=self.name,
            root=self.root,
            steps=tuple(steps),
            project=self.project,
            dataset=self.dataset,
            max_workers=self.max_workers,
        )


def discover(jobs_dir: str | Path = "jobs") -> dict[str, Path]:
    """Map job name -> ``job.toml`` path for every job under *jobs_dir*."""
    root = Path(jobs_dir)
    if not root.is_dir():
        raise ManifestError(f"jobs directory not found: {root}")
    found = {path.parent.name: path for path in sorted(root.glob("*/job.toml"))}
    if not found:
        raise ManifestError(f"no jobs found under {root} (expected <job>/job.toml)")
    return found


def load_job(name: str, jobs_dir: str | Path = "jobs") -> Job:
    """Load one job by name."""
    jobs = discover(jobs_dir)
    if name not in jobs:
        raise ManifestError(f"unknown job '{name}' (have: {', '.join(sorted(jobs))})")
    return Job.load(jobs[name])
