"""Dry-run mode: validate every step without executing it."""

from pipeline.manifest import Job, Step
from pipeline.runner import JobRunner, run_jobs


class DryRun:
    def __init__(self, statement_type="CREATE_TABLE_AS_SELECT", total_bytes_processed=1000):
        self.statement_type = statement_type
        self.total_bytes_processed = total_bytes_processed


class FakeClient:
    project_id = "test-project"

    def __init__(self):
        self.executed = []
        self.previewed = []

    def execute(
        self,
        sql,
        timeout=None,
        parameters=None,
        return_dataframe=False,
        max_bytes_billed=None,
        job_name=None,
    ):
        raise AssertionError("execute() must not be called in dry-run mode")

    def dry_run(self, sql, parameters=None):
        self.previewed.append(parameters)
        return DryRun()


def make_job(tmp_path, name="demo", steps=(("a", ()), ("b", ("a",)))):
    root = tmp_path / name
    root.mkdir(parents=True, exist_ok=True)
    built = []
    for step_name, deps in steps:
        path = root / f"{step_name}.sql"
        path.write_text(f"SELECT '{step_name}'", encoding="utf-8")
        built.append(Step(step_name, path, tuple(deps)))
    return Job(name=name, root=root, steps=tuple(built), dataset="ds")


class TestDryRun:
    def test_previews_every_step_and_executes_none(self, tmp_path):
        client = FakeClient()
        batch = run_jobs(
            [make_job(tmp_path)],
            JobRunner(client, dry_run=True),
            project="p",
            max_workers=1,
        )

        assert len(client.previewed) == 2
        assert client.previewed[0] == {}
        assert [result.state for result in batch.reports[0].results] == ["DRY_RUN", "DRY_RUN"]
        assert batch.dry_run is True

    def test_reports_estimated_bytes_and_no_job_ids(self, tmp_path):
        batch = run_jobs(
            [make_job(tmp_path)],
            JobRunner(FakeClient(), dry_run=True),
            project="p",
            max_workers=1,
        )

        report = batch.reports[0]
        assert report.ok
        assert report.total_bytes == 2000
        assert all(result.job_id is None for result in report.results)
        assert "total estimated 1.95 KiB" in batch.summary()

    def test_parameters_are_passed_to_the_preview(self, tmp_path):
        client = FakeClient()
        run_jobs(
            [make_job(tmp_path)],
            JobRunner(client, dry_run=True),
            project="p",
            parameters={"run_date": "2024-01-01"},
            max_workers=1,
        )
        assert client.previewed[0] == {"run_date": "2024-01-01"}

    def test_real_runs_are_unaffected(self, tmp_path):
        class Executing(FakeClient):
            def execute(
                self,
                sql,
                timeout=None,
                parameters=None,
                return_dataframe=False,
                max_bytes_billed=None,
                job_name=None,
            ):
                self.executed.append(job_name)
                return type(
                    "R",
                    (),
                    {
                        "job_id": "job-1",
                        "statement_type": "CREATE_TABLE_AS_SELECT",
                        "num_dml_affected_rows": None,
                        "total_bytes_processed": 10,
                    },
                )()

        client = Executing()
        batch = run_jobs([make_job(tmp_path)], JobRunner(client), project="p", max_workers=1)

        assert len(client.executed) == 2
        assert client.previewed == []
        assert [result.state for result in batch.reports[0].results] == ["DONE", "DONE"]
        assert batch.dry_run is False

    def test_dry_run_failure_is_reported_not_raised(self, tmp_path):
        class Failing(FakeClient):
            def dry_run(self, sql, parameters=None):
                raise ValueError("Syntax error: Unrecognized name")

        batch = run_jobs([make_job(tmp_path)], JobRunner(Failing(), dry_run=True), project="p")

        assert not batch.ok
        assert "Unrecognized name" in batch.reports[0].error
        assert any(result.state == "FAILED" for result in batch.reports[0].results)
