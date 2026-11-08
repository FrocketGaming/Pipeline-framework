"""Manifest loading and DAG shaping. No BigQuery, no credentials."""

import pytest

from pipeline.manifest import (
    Job,
    ManifestError,
    discover,
    load_job,
    parse_bytes,
    render,
)

MANIFEST = """
dataset = "ds"

[[step]]
name = "a"
file = "queries/01_a.sql"

[[step]]
name = "b"
file = "queries/02_b.sql"

[[step]]
name = "c"
file = "queries/03_c.sql"
depends_on = ["a", "b"]

[[step]]
name = "d"
file = "queries/04_d.sql"
depends_on = ["c"]
"""

DIAMOND = """
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
depends_on = ["a"]

[[step]]
name = "d"
file = "queries/04_d.sql"
depends_on = ["b", "c"]
"""

CYCLE = """
[[step]]
name = "a"
file = "queries/01_a.sql"
depends_on = ["b"]

[[step]]
name = "b"
file = "queries/02_b.sql"
depends_on = ["a"]
"""

FILES = {
    "01_a.sql": "SELECT 1",
    "02_b.sql": "SELECT 2",
    "03_c.sql": "SELECT 3",
    "04_d.sql": "SELECT 4",
}


def make_job(tmp_path, manifest=MANIFEST, files=None, name="demo"):
    job_dir = tmp_path / "jobs" / name
    (job_dir / "queries").mkdir(parents=True, exist_ok=True)
    for filename, sql in (FILES if files is None else files).items():
        (job_dir / "queries" / filename).write_text(sql, encoding="utf-8")
    target = job_dir / "job.toml"
    target.write_text(manifest, encoding="utf-8")
    return target


class TestLevels:
    def test_fan_in(self, tmp_path):
        job = Job.load(make_job(tmp_path))
        assert [[step.name for step in level] for level in job.levels()] == [
            ["a", "b"],
            ["c"],
            ["d"],
        ]

    def test_diamond(self, tmp_path):
        job = Job.load(make_job(tmp_path, DIAMOND, name="diamond"))
        assert [[step.name for step in level] for level in job.levels()] == [
            ["a"],
            ["b", "c"],
            ["d"],
        ]

    def test_name_comes_from_directory(self, tmp_path):
        job = Job.load(make_job(tmp_path, name="alpha_report"))
        assert job.name == "alpha_report"

    def test_dataset_from_manifest(self, tmp_path):
        assert Job.load(make_job(tmp_path)).dataset == "ds"


class TestValidation:
    def test_rejects_cycle(self, tmp_path):
        with pytest.raises(ManifestError, match="cycle"):
            Job.load(make_job(tmp_path, CYCLE, name="cycle"))

    def test_rejects_unknown_dependency(self, tmp_path):
        manifest = MANIFEST.replace('depends_on = ["a", "b"]', 'depends_on = ["ghost"]')
        with pytest.raises(ManifestError, match="unknown step"):
            Job.load(make_job(tmp_path, manifest))

    def test_rejects_missing_file(self, tmp_path):
        files = dict(FILES)
        files.pop("03_c.sql")
        with pytest.raises(ManifestError, match="missing file"):
            Job.load(make_job(tmp_path, files=files))

    def test_rejects_duplicate_step_names(self, tmp_path):
        manifest = """
[[step]]
name = "a"
file = "queries/01_a.sql"

[[step]]
name = "a"
file = "queries/02_b.sql"
"""
        with pytest.raises(ManifestError, match="duplicate step name"):
            Job.load(make_job(tmp_path, manifest, name="dupe"))

    def test_rejects_unknown_key(self, tmp_path):
        manifest = MANIFEST.replace('dataset = "ds"', 'dataset = "ds"\nbogus = 1')
        with pytest.raises(ManifestError, match="unknown key"):
            Job.load(make_job(tmp_path, manifest))

    def test_rejects_name_key_in_favour_of_directory(self, tmp_path):
        manifest = MANIFEST.replace('dataset = "ds"', 'dataset = "ds"\nname = "other"')
        with pytest.raises(ManifestError, match="unknown key"):
            Job.load(make_job(tmp_path, manifest))

    def test_rejects_empty_manifest(self, tmp_path):
        with pytest.raises(ManifestError, match="no \\[\\[step\\]\\]"):
            Job.load(make_job(tmp_path, 'dataset = "ds"', name="empty"))


class TestRenderAndUnits:
    def test_substitutes_identifiers(self):
        sql = render(
            "SELECT * FROM `{{ project }}.{{ dataset }}.t`",
            {"project": "my-proj", "dataset": "ds"},
        )
        assert sql == "SELECT * FROM `my-proj.ds.t`"

    def test_leaves_query_parameters_alone(self):
        sql = "SELECT * FROM `{{ project }}.{{ dataset }}.t` WHERE d >= @run_date"
        assert "@run_date" in render(sql, {"project": "p", "dataset": "d"})

    def test_rejects_injection(self):
        with pytest.raises(ManifestError, match="invalid identifier"):
            render("SELECT * FROM {{ dataset }}", {"dataset": "ds; DROP TABLE x"})

    def test_rejects_unresolved_placeholder(self):
        with pytest.raises(ManifestError, match="unresolved"):
            render("SELECT {{ nope }}", {})

    @pytest.mark.parametrize(
        "value,expected",
        [(1024, 1024), ("1KiB", 1024), ("50GiB", 50 * 1024**3), ("1GB", 10**9)],
    )
    def test_parse_bytes(self, value, expected):
        assert parse_bytes(value) == expected


class TestDiscovery:
    def test_discovers_every_job(self, tmp_path):
        make_job(tmp_path, name="alpha")
        make_job(tmp_path, name="beta")
        jobs_dir = tmp_path / "jobs"
        assert sorted(discover(jobs_dir)) == ["alpha", "beta"]
        assert load_job("alpha", jobs_dir).name == "alpha"

    def test_unknown_job_lists_alternatives(self, tmp_path):
        make_job(tmp_path, name="alpha")
        with pytest.raises(ManifestError, match="unknown job 'nope' .*alpha"):
            load_job("nope", tmp_path / "jobs")

    def test_missing_jobs_dir(self, tmp_path):
        with pytest.raises(ManifestError, match="jobs directory not found"):
            discover(tmp_path / "nope")
