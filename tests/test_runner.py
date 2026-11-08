"""Runner behaviour: ordering, fail-fast, and job isolation. No BigQuery needed."""

import pytest

from pipeline.manifest import Job, JobError, Step
from pipeline.runner import StepFailed, run_job, run_jobs


class FakeError(Exception):
    def __init__(self, message, job_id="job-bad", bytes_processed=0):
        super().__init__(message)
        self.job_id = job_id
        self.bytes_processed = bytes_processed


class FakeResult:
    """Mirrors zbq.main.JobResult."""

    def __init__(
        self,
        job_id,
        statement_type="CREATE_TABLE_AS_SELECT",
        rows=None,
        bytes_processed=100,
    ):
        self.job_id = job_id
        self.statement_type = statement_type
        self.num_dml_affected_rows = rows
        self.total_bytes_processed = bytes_processed


class FakeClient:
    """Same surface as ``zbq.BigQueryHandler.execute``."""

    def __init__(self, fail_on=()):
        self.fail_on = set(fail_on)
        self.calls = []

    def execute(
        self,
        sql,
        timeout=None,
        parameters=None,
        return_dataframe=False,
        max_bytes_billed=None,
        job_name=None,
    ):
        self.calls.append(
            {
                "job_name": job_name,
                "sql": sql,
                "parameters": parameters,
                "timeout": timeout,
                "max_bytes": max_bytes_billed,
                "return_dataframe": return_dataframe,
            }
        )
        if job_name in self.fail_on:
            raise FakeError("boom", job_id="job-bad")
        return FakeResult("job-" + job_name)


def steps_of(calls):
    """``demo.a`` -> ``a``."""
    return [call["job_name"].split(".", 1)[1] for call in calls]


def make_job(tmp_path, name="demo", steps=(("a", ()), ("b", ()), ("c", ("a", "b")))):
    root = tmp_path / name
    root.mkdir(parents=True, exist_ok=True)
    built = []
    for step_name, deps in steps:
        path = root / f"{step_name}.sql"
        path.write_text(f"SELECT '{step_name}'", encoding="utf-8")
        built.append(Step(step_name, path, tuple(deps)))
    return Job(name=name, root=root, steps=tuple(built), dataset="ds")


class TestRunJob:
    def test_runs_levels_in_order(self, tmp_path):
        client = FakeClient()
        report = run_job(make_job(tmp_path), client, project="p", max_workers=1)
        assert steps_of(client.calls) == ["a", "b", "c"]
        assert report.ok
        assert report.total_bytes == 300
        assert client.calls[0]["return_dataframe"] is False

    def test_forwards_job_name_parameters_and_guards(self, tmp_path):
        client = FakeClient()
        job = make_job(tmp_path)
        run_job(
            job,
            client,
            project="p",
            parameters={"run_date": "2024-01-01"},
            run_id="r1",
            max_workers=1,
        )
        first = client.calls[0]
        assert first["job_name"] == "demo.a"
        assert first["parameters"] == {"run_date": "2024-01-01"}
        assert first["sql"].startswith("SELECT 'a'")

    def test_step_guards_forwarded(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1", encoding="utf-8")
        job = Job(
            name="demo",
            root=tmp_path,
            steps=(Step("a", path, timeout=60, max_bytes=1024),),
            dataset="ds",
        )
        client = FakeClient()
        run_job(job, client, project="p")
        assert client.calls[0]["timeout"] == 60
        assert client.calls[0]["max_bytes"] == 1024

    def test_failure_stops_level_and_downstream(self, tmp_path):
        client = FakeClient(fail_on={"demo.b"})
        job = make_job(tmp_path, steps=(("a", ()), ("b", ()), ("c", ("b",))))
        with pytest.raises(StepFailed) as excinfo:
            run_job(job, client, project="p", max_workers=1)

        assert steps_of(client.calls) == ["a", "b"]
        report = excinfo.value.report
        assert report is not None
        assert "'b'" in report.error
        assert any(result.state == "FAILED" for result in report.results)

    def test_job_without_dataset_raises(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1", encoding="utf-8")
        job = Job(name="x", root=tmp_path, steps=(Step("a", path),), dataset="")
        with pytest.raises(JobError, match="no dataset"):
            run_job(job, FakeClient(), project="p")


class TestRunJobs:
    def test_one_failure_does_not_stop_other_jobs(self, tmp_path):
        good = make_job(tmp_path, "good", steps=(("a", ()),))
        broken = make_job(tmp_path, "broken", steps=(("a", ()),))
        client = FakeClient(fail_on={"broken.a"})

        batch = run_jobs([good, broken], client, project="p", max_jobs=2, max_workers=1)

        assert not batch.ok
        assert batch.failed == ["broken"]
        by_name = {report.job: report for report in batch.reports}
        assert by_name["good"].ok
        assert not by_name["broken"].ok
        assert {call["job_name"] for call in client.calls} == {"good.a", "broken.a"}

    def test_all_jobs_reported_in_name_order(self, tmp_path):
        jobs = [make_job(tmp_path, name, steps=(("a", ()),)) for name in ("two", "one", "three")]
        batch = run_jobs(jobs, FakeClient(), project="p", max_jobs=3, max_workers=1)
        assert batch.ok
        assert [report.job for report in batch.reports] == ["one", "three", "two"]

    def test_empty_batch_is_ok(self):
        assert run_jobs([], FakeClient(), project="p").ok

    def test_summary_points_at_the_failing_job_step_and_file(self, tmp_path):
        broken = make_job(tmp_path, "broken", steps=(("a", ()), ("b", ())))
        batch = run_jobs([broken], FakeClient(fail_on={"broken.b"}), project="p", max_workers=1)

        text = batch.summary()
        assert "broken" in text
        assert "FAILED" in text
        assert "1/2" in text  # one of two steps ran
        assert "step 'b'" in text
        assert "b.sql" in text
        assert "boom" in text


class TestFailureDetail:
    def test_report_names_the_step_file_and_error(self, tmp_path):
        client = FakeClient(fail_on={"demo.b"})
        job = make_job(tmp_path, steps=(("a", ()), ("b", ())))
        with pytest.raises(StepFailed) as excinfo:
            run_job(job, client, project="p", max_workers=1)

        report = excinfo.value.report
        assert report is not None
        assert report.progress == "1/2"
        assert "step 'b'" in report.error
        assert "b.sql" in report.error
        assert "boom" in report.error
