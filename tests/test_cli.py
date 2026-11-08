"""CLI behaviour and partial (single-step) runs. No BigQuery needed."""

import pytest
from typer.testing import CliRunner

from pipeline.cli import app
from pipeline.manifest import Job, ManifestError

runner = CliRunner()

MANIFEST = """
dataset = "ds"

[[step]]
name = "a"
file = "queries/01_a.sql"

[[step]]
name = "b"
file = "queries/02_b.sql"
depends_on = ["a"]

[[step]]
name = "c"
file = "queries/03_c.sql"
depends_on = ["b"]
"""


def make_jobs(tmp_path, name="demo", manifest=MANIFEST):
    job_dir = tmp_path / "jobs" / name
    (job_dir / "queries").mkdir(parents=True, exist_ok=True)
    for filename in ("01_a.sql", "02_b.sql", "03_c.sql"):
        (job_dir / "queries" / filename).write_text("SELECT 1", encoding="utf-8")
    (job_dir / "job.toml").write_text(manifest, encoding="utf-8")
    return tmp_path / "jobs"


class TestCli:
    def test_list(self, tmp_path):
        jobs = make_jobs(tmp_path)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "list"])
        assert result.exit_code == 0
        assert "demo" in result.stdout
        assert "3 steps" in result.stdout

    def test_validate_ok(self, tmp_path):
        jobs = make_jobs(tmp_path)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "validate"])
        assert result.exit_code == 0
        assert "demo: ok (3 steps, 3 levels)" in result.stdout

    def test_validate_reports_broken_job(self, tmp_path):
        jobs = make_jobs(tmp_path, manifest='dataset = "ds"\n[[step]]\nname = "a"\n')
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "validate"])
        assert result.exit_code == 1
        assert "INVALID" in result.stdout

    def test_plan_shows_levels(self, tmp_path):
        jobs = make_jobs(tmp_path)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "plan", "demo"])
        assert result.exit_code == 0
        assert "level 1" in result.stdout
        assert "interval" not in result.stdout
        assert "01_a.sql" in result.stdout

    def test_run_needs_a_job(self, tmp_path):
        jobs = make_jobs(tmp_path)
        result = runner.invoke(app, ["--jobs-dir", str(jobs), "run"])
        assert result.exit_code == 2
        assert "pass --job" in result.stderr

    def test_step_requires_one_job(self, tmp_path):
        jobs = make_jobs(tmp_path)
        jobs = make_jobs(tmp_path, "other")
        result = runner.invoke(
            app,
            [
                "--jobs-dir",
                str(jobs),
                "run",
                "--job",
                "demo",
                "--job",
                "other",
                "--step",
                "a",
            ],
        )
        assert result.exit_code == 2
        assert "exactly one --job" in result.stderr


class TestSubset:
    def test_single_step_drops_its_dependencies(self, tmp_path):
        jobs = make_jobs(tmp_path)
        job = Job.load(jobs / "demo" / "job.toml")

        subset = job.subset(["b"])

        assert [step.name for step in subset.steps] == ["b"]
        assert subset.steps[0].depends_on == ()
        assert [[step.name for step in level] for level in subset.levels()] == [["b"]]
        assert subset.dataset == "ds"

    def test_with_dependencies_pulls_in_ancestors(self, tmp_path):
        jobs = make_jobs(tmp_path)
        job = Job.load(jobs / "demo" / "job.toml")

        subset = job.subset(["c"], with_dependencies=True)

        assert [step.name for step in subset.steps] == ["a", "b", "c"]
        assert [[step.name for step in level] for level in subset.levels()] == [
            ["a"],
            ["b"],
            ["c"],
        ]

    def test_unknown_step_is_rejected(self, tmp_path):
        jobs = make_jobs(tmp_path)
        job = Job.load(jobs / "demo" / "job.toml")
        with pytest.raises(ManifestError, match="unknown step"):
            job.subset(["ghost"])

    def test_subset_keeps_step_guards(self, tmp_path):
        manifest = MANIFEST.replace(
            'file = "queries/01_a.sql"', 'file = "queries/01_a.sql"\ntimeout = 60'
        )
        jobs = make_jobs(tmp_path, manifest=manifest)
        job = Job.load(jobs / "demo" / "job.toml")
        assert job.subset(["a"]).steps[0].timeout == 60
