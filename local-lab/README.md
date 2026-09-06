# Local Lab — RetailCo pipeline, no Docker, no dead API

This folder is a self-contained, laptop-only version of the pipeline
described in the [root README](../README.md). It exists because two things
the original design depended on aren't available anymore:

- the ERP REST API (`hngstage8da-...herokuapp.com`) is gone
- Docker isn't installed on this machine

Same three stages, same shape, different plumbing:

| Stage | Original | Here |
|---|---|---|
| Extract | `extractor/erp_extractor.py` calling a live API | [`fake_erp_generator.py`](fake_erp_generator.py) — generates realistic data with Faker, same column names your `stg_*.sql` models expect |
| Load | `dlt_pipeline/dlt_pipeline.py`, Postgres → Postgres | [`dlt_pipeline_local.py`](dlt_pipeline_local.py) — same merge/incremental dlt logic, DuckDB → DuckDB |
| Transform | `retailco_dbt/` against Postgres | The **same, unmodified `retailco_dbt/` project**, pointed at a new `duckdb_local` target instead of `dev` (Postgres) |

Nothing in `retailco_dbt/models` was rewritten. Two small `{% if target.type == 'duckdb' %}` branches were added to
[`../retailco_dbt/models/marts/dimensions/dim_date.sql`](../retailco_dbt/models/marts/dimensions/dim_date.sql)
because `to_char()` and `generate_series()` behave differently on Postgres vs DuckDB — the Postgres branch is untouched, so the original project still runs unmodified against real Postgres.

## Setup (one-time)

Two virtual environments — one for the extract/load scripts (dlt conflicts with dbt's pinned deps if installed together), one for dbt:

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install faker duckdb "dlt[duckdb]"

python -m venv .venv-dbt
./.venv-dbt/Scripts/python.exe -m pip install dbt-duckdb==1.9.4
```

## Run the pipeline

```bash
# 1. Extract: generate fake ERP data into lake.duckdb
./.venv/Scripts/python.exe fake_erp_generator.py

# 2. Load: lake.duckdb -> warehouse.duckdb (merge/incremental)
./.venv/Scripts/python.exe dlt_pipeline_local.py

# 3. Transform: staging -> snapshots -> marts -> tests
DBT_PROFILES_DIR=. ./.venv-dbt/Scripts/dbt.exe run --project-dir ../retailco_dbt --target duckdb_local --select staging
DBT_PROFILES_DIR=. ./.venv-dbt/Scripts/dbt.exe snapshot --project-dir ../retailco_dbt --target duckdb_local
DBT_PROFILES_DIR=. ./.venv-dbt/Scripts/dbt.exe run --project-dir ../retailco_dbt --target duckdb_local --select marts
DBT_PROFILES_DIR=. ./.venv-dbt/Scripts/dbt.exe test --project-dir ../retailco_dbt --target duckdb_local
```

All 58 tests from the original project pass against this data.

## Query the warehouse

```python
import duckdb
con = duckdb.connect("warehouse.duckdb", read_only=True)
con.sql("select * from raw_marts.fct_sales limit 10").show()
```

Or use any of the "Business Questions Queries" from the root README directly — they run unmodified against `raw_marts.*` here.

## Re-running / getting fresh data

`fake_erp_generator.py` fully rebuilds `lake.duckdb` from scratch each time (it's not incremental — it's standing in for a one-time ERP snapshot). To simulate a second day's incremental load, edit the generator to append new/updated rows with a later `updated_at` instead of rebuilding, then re-run steps 2–3 — `dlt`'s incremental hint on `updated_at` and dbt's snapshot `strategy='timestamp'` will pick up only what changed, exactly like the real pipeline.
