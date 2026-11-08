# pipeline

Runs SQL jobs against BigQuery. A job is a directory under `jobs/`: a
`job.toml` that names the steps and their dependencies, plus one `.sql` file per
step. Several jobs can run at once from one command.

```
pipeline/
  src/pipeline/
    manifest.py    # job.toml -> Job, levels()
    runner.py      # levels + jobs, concurrency, reports
    cli.py         # pipeline list|validate|plan|run
  jobs/
    daily_orders/
      job.toml
      queries/01_stg_orders.sql
      queries/02_stg_users.sql
      queries/03_fact_orders.sql
    finance_sync/
      job.toml
      queries/...
```

## Commands

```bash
pipeline list                        # jobs and their step counts
pipeline validate                    # check every job.toml, no BigQuery needed
pipeline plan daily_orders           # show the execution levels
pipeline run --job daily_orders --var run_date=2024-01-01
pipeline run --job daily_orders --job finance_sync     # both, concurrently
pipeline run --all --max-jobs 2

# validate without writing anything: BigQuery checks the SQL, nothing is billed
pipeline run --job daily_orders --dry-run
pipeline run --job daily_orders --step fact_orders --dry-run

# or actually build into a scratch dataset
pipeline run --job daily_orders --step stg_orders --dataset scratch --var run_date=2024-01-01
pipeline run --job daily_orders --step fact_orders --with-deps
```

Exit codes: `0` all jobs succeeded, `1` any job failed, `2` bad manifest or usage.

## Testing a single step

`--step` narrows a job to the steps you name, so you can iterate on one query
without re-running the whole job. By default the step runs alone — its
`depends_on` entries are dropped, which assumes the upstream tables already
exist. Add `--with-deps` to run the named steps and everything they depend on.

```bash
pipeline plan daily_orders                              # names available to --step
pipeline run --job daily_orders --step stg_orders       # just this query
pipeline run --job daily_orders --step fact_orders --with-deps
```

`--step` requires exactly one `--job`. Pair it with `--dataset` to point at a
scratch dataset so test runs never touch production tables.

## job.toml

The directory name is the job id: `jobs/daily_orders` is run with
`pipeline run --job daily_orders`.

```toml
project = "my-project"     # optional; overridden by --project
dataset = "analytics"      # required unless you pass --dataset

[[step]]
name = "stg_orders"
file = "queries/01_stg_orders.sql"
timeout = 900              # optional, seconds -> execute(timeout=...)
max_bytes = "50GiB"        # optional -> execute(max_bytes_billed=...)

[[step]]
name = "stg_users"
file = "queries/02_stg_users.sql"

[[step]]
name = "fact_orders"
file = "queries/03_fact_orders.sql"
depends_on = ["stg_orders", "stg_users"]   # runs after both
```

`depends_on` is what expresses parallelism: steps without a dependency between
them run at the same time, and a step waits for *all* of its dependencies.

## SQL files

One statement per file. Two kinds of placeholder:

- `{{ project }}` / `{{ dataset }}` — identifiers, validated and substituted by
  the runner. Never data.
- `@run_date` — a real BigQuery query parameter, passed with `--var run_date=...`.

Every step should be idempotent (`CREATE OR REPLACE TABLE ... AS SELECT`), so a
failed job can simply be re-run.

## Concurrency

- Jobs run in parallel, capped by `--max-jobs`.
- Steps within a job run in parallel per level, capped by `--max-workers`.
- One shared semaphore (`--max-concurrency`, default 8) caps in-flight BigQuery
  jobs across everything, so a wide batch doesn't blow the concurrent-job quota.

## Reports and attribution

Every step is one `zbq` `execute()` call, so each returns a `JobResult`
(`job_id`, `statement_type`, `num_dml_affected_rows`, `total_bytes_processed`)
and the run ends with a per-job table of steps, bytes, and job ids.

Pass `--status-table proj.dataset.runs` and zbq appends one row per step
(`status`, `duration_ms`, `bytes_billed`, `rows_affected`, `error`, `job_name`)
so run history lives in BigQuery rather than only in logs. The `job_name` label
is `<job>.<step>`.

## Failure

A failing step stops its level (queued steps are dropped) and no later level
starts. Other jobs are unaffected; the batch reports per-job status and exits
`1` if any job failed.

Steps already running when a sibling fails are left to finish — `zbq.execute()`
is blocking and exposes no cancel handle. A step that times out raises and stops
the job, but BigQuery may still complete that statement afterwards, which is
another reason every step should be `CREATE OR REPLACE` and therefore re-runnable.

Already-committed steps stay committed — BigQuery is atomic per statement, not
per job — so a failed run leaves earlier tables updated and later ones stale.
The next successful run converges.

## zbq dependency

`pyproject.toml` points at the local checkout:

```toml
[tool.uv.sources]
zbq = { path = "../zbq", editable = true }
```

The runner only needs:

```python
client.execute(sql, timeout=None, parameters=None, max_bytes_billed=None, job_name=None)
```

which returns a job result with `job_id`, `statement_type`,
`num_dml_affected_rows`, and `total_bytes_processed`. It never calls `read()`,
so result rows are never pulled into this process.
