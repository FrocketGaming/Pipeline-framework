"""``project`` declared in job.toml, overridden by ``--project``."""

import pytest
from typer.testing import CliRunner

from pipeline.cli import app
from pipeline.manifest import Job, ManifestError

runner = CliRunner()

BASE = """
project = "proj-a"
dataset = "ds"

[[step]]
name = "a"
file = "queries/01_a.sql"
"""


def write_job(root, manifest, name="demo"):
    job_dir = root / name
    (job_dir / "queries").mkdir(parents=True, exist_ok=True)
    (job_dir / "queries" / "01_a.sql").write_text("SELECT 1", encoding="utf-8")
    target = job_dir / "job.toml"
    target.write_text(manifest, encoding="utf-8")
    return target


class TestProjectInToml:
    def test_project_is_loaded(self, tmp_path):
        assert Job.load(write_job(tmp_path, BASE)).project == "proj-a"

    def test_project_is_optional(self, tmp_path):
        manifest = BASE.replace('project = "proj-a"\n', "")
        assert Job.load(write_job(tmp_path, manifest)).project == ""

    def test_project_must_be_a_string(self, tmp_path):
        manifest = BASE.replace('project = "proj-a"', "project = 42")
        with pytest.raises(ManifestError, match="'project' must be a string"):
            Job.load(write_job(tmp_path, manifest))

    def test_subset_keeps_project(self, tmp_path):
        job = Job.load(write_job(tmp_path, BASE))
        assert job.subset(["a"]).project == "proj-a"

    def test_jobs_in_different_projects_are_rejected(self, tmp_path):
        jobs = tmp_path / "jobs"
        write_job(jobs, BASE, name="one")
        write_job(jobs, BASE.replace("proj-a", "proj-b"), name="two")

        result = runner.invoke(
            app, ["--jobs-dir", str(jobs), "run", "--job", "one", "--job", "two"]
        )

        assert result.exit_code == 2
        assert "different projects" in result.stderr
        assert "proj-a" in result.stderr and "proj-b" in result.stderr
