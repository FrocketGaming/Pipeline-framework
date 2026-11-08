"""Regression tests for reported bugs: manifest typing, CLI errors, dry-run skip."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from pipeline.cli import app
from pipeline.manifest import Job, ManifestError, Step
from pipeline.runner import JobRunner, run_jobs

runner = CliRunner()

VALID_MANIFEST = """
dataset = "ds"

[[step]]
name = "a"
file = "queries/01_a.sql"

[[step]]
name = "b"
file = "queries/02_b.sql"
depends_on = ["a"]
"""


def write_job(root: Path, manifest: str, *, name: str = "demo") -> Path:
    job_dir = root / name
    (job_dir / "queries").mkdir(parents=True, exist_ok=True)
    for filename in ("01_a.sql", "02_b.sql"):
        (job_dir / "queries" / filename).write_text("SELECT 1", encoding="utf-8")
    target = job_dir / "job.toml"
    target.write_text(manifest, encoding="utf-8")
    return target


class TestManifestTyping:
    def test_depends_on_string_is_rejected(self, tmp_path):
        manifest = VALID_MANIFEST.replace('depends_on = ["a"]', 'depends_on = "a"')
        with pytest.raises(ManifestError, match="must be a list of step names"):
            Job.load(write_job(tmp_path, manifest))

    def test_timeout_string_is_rejected(self, tmp_path):
        manifest = VALID_MANIFEST.replace(
            'file = "queries/01_a.sql"', 'file = "queries/01_a.sql"\ntimeout = "60"'
        )
        with pytest.raises(ManifestError, match="timeout.*integer"):
            Job.load(write_job(tmp_path, manifest))

    def test_step_string_is_rejected(self, tmp_path):
        manifest = 'dataset = "ds"\nstep = "a"\n'
        with pytest.raises(ManifestError, match="array of tables"):
            Job.load(write_job(tmp_path, manifest))


class TestCliErrors:
    def test_list_missing_jobs_dir_is_friendly(self, tmp_path):
        result = runner.invoke(app, ["--jobs-dir", str(tmp_path / "nope"), "list"])
        assert result.exit_code == 2
        assert "jobs directory not found" in result.stderr
        assert "Traceback" not in result.stderr

    def test_validate_missing_jobs_dir_is_friendly(self, tmp_path):
        result = runner.invoke(app, ["--jobs-dir", str(tmp_path / "nope"), "validate"])
        assert result.exit_code == 2
        assert "jobs directory not found" in result.stderr

    def test_run_all_missing_jobs_dir_is_friendly(self, tmp_path):
        result = runner.invoke(app, ["--jobs-dir", str(tmp_path / "nope"), "run", "--all"])
        assert result.exit_code == 2
        assert "jobs directory not found" in result.stderr

    def test_job_and_all_are_mutually_exclusive(self, tmp_path):
        jobs = tmp_path / "jobs"
        write_job(jobs, VALID_MANIFEST)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "run", "--job", "demo", "--all"])
        assert result.exit_code == 2
        assert "mutually exclusive" in result.stderr

    def test_with_deps_requires_step(self, tmp_path):
        jobs = tmp_path / "jobs"
        write_job(jobs, VALID_MANIFEST)
        result = runner.invoke(
            app, ["--jobs-dir", str(jobs), "run", "--job", "demo", "--with-deps"]
        )
        assert result.exit_code == 2
        assert "requires --step" in result.stderr

    def test_step_with_all_is_rejected(self, tmp_path):
        jobs = tmp_path / "jobs"
        write_job(jobs, VALID_MANIFEST)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "run", "--all", "--step", "a"])
        assert result.exit_code == 2
        assert "--step cannot be used with --all" in result.stderr


class NotFound(Exception):
    pass


class DryRun:
    statement_type = "CREATE_TABLE_AS_SELECT"
    total_bytes_processed = 100


class MixedClient:
    """Succeeds for creators, raises NotFound for the named reader table."""

    def __init__(self, missing_table: str):
        self.missing_table = missing_table

    def dry_run(self, sql, parameters=None):
        if f"`p.d.{self.missing_table}`" in sql and "SELECT * FROM" in sql:
            raise NotFound(
                f"404 Not found: Table p:d.{self.missing_table} was not found in location US"
            )
        return DryRun()

    def execute(self, *args, **kwargs):
        raise AssertionError("execute() must not run in dry-run mode")


def make_sql_job(tmp_path, reader_table: str) -> Job:
    root = tmp_path / "job"
    root.mkdir(parents=True)
    creator = root / "a.sql"
    creator.write_text("CREATE OR REPLACE TABLE `p.d.stg_a` AS SELECT 1", encoding="utf-8")
    reader = root / "b.sql"
    reader.write_text(f"SELECT * FROM `p.d.{reader_table}`", encoding="utf-8")
    return Job(
        name="job",
        root=root,
        steps=(Step("a", creator, ()), Step("b", reader, ("a",))),
        dataset="d",
    )


class TestDryRunSkip:
    def test_reads_of_sibling_outputs_are_skipped(self, tmp_path):
        job = make_sql_job(tmp_path, reader_table="stg_a")  # stg_a is created by step a
        batch = run_jobs(
            [job], JobRunner(MixedClient("stg_a"), dry_run=True), project="p", max_workers=1
        )

        report = batch.reports[0]
        assert report.ok
        by_step = {result.step: result for result in report.results}
        assert by_step["a"].state == "DRY_RUN"
        assert by_step["b"].state == "SKIPPED"
        assert "stg_a" in by_step["b"].note

    def test_unknown_table_still_fails(self, tmp_path):
        job = make_sql_job(tmp_path, reader_table="stg_typo")  # not created by this job
        batch = run_jobs(
            [job], JobRunner(MixedClient("stg_typo"), dry_run=True), project="p", max_workers=1
        )

        assert not batch.ok
        assert "stg_typo" in batch.reports[0].error
        assert any(result.state == "FAILED" for result in batch.reports[0].results)

    def test_missing_dataset_is_skipped_with_reason(self, tmp_path):
        class MissingDataset(MixedClient):
            def dry_run(self, sql, parameters=None):
                if "SELECT * FROM" in sql:
                    raise NotFound("404 Not found: Dataset p:nothere was not found")
                return DryRun()

        job = make_sql_job(tmp_path, reader_table="stg_a")
        batch = run_jobs(
            [job], JobRunner(MissingDataset("stg_a"), dry_run=True), project="p", max_workers=1
        )

        report = batch.reports[0]
        assert report.ok
        by_step = {result.step: result for result in report.results}
        assert by_step["b"].state == "SKIPPED"
        assert "dataset 'nothere'" in by_step["b"].note
