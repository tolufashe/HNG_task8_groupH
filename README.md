# RetailCo Data Pipeline

This repository contains the full RetailCo data platform built with Apache Airflow, dlt, and dbt.
The pipeline extracts data from the RetailCo ERP API, loads it into a data warehouse, and transforms
it into analytics-ready dimensional models.

---

## What this does

The pipeline runs in four stages:

1. **Extract** — pulls all 9 business entities from the ERP API into a raw lake PostgreSQL database
2. **Load** — moves data from the lake into the warehouse using dlt, with type coercion and snake_case renaming
3. **Transform** — dbt staging models clean the data; dbt mart models build Kimball-style dimensions and facts
4. **Orchestrate** — Airflow runs the full pipeline daily with retries and failure handling

---

## Prerequisites

Before running anything, make sure you have the following installed:

- **Docker Desktop**: https://www.docker.com/products/docker-desktop
- **VS Code**: https://code.visualstudio.com
- **Python 3.11+**: https://www.python.org/downloads

Verify Docker is running by opening Docker Desktop and confirming the whale icon appears
in your taskbar with the status "Engine running".

---

## Step 1: Clone the repository

```bash
git clone https://github.com/tolufashe/HNG_task8_groupH.git
cd retailco-pipeline
```

---

## Step 2: Configure the `.env` file

Create a file called `.env` in the root `retailco-pipeline` folder.
This file holds all secrets and connection details. **Never commit this file to Git.**

```
# ERP API credentials
ERP_API_KEY=your_api_key_here
ERP_API_BASE_URL=https://hngstage8da-55c7f5f769c8.herokuapp.com

# Lake database (raw data storage)
LAKE_DB_HOST=lake_postgres
LAKE_DB_PORT=5432
LAKE_DB_NAME=lake
LAKE_DB_USER=lake_user
LAKE_DB_PASSWORD=lake_pass

# Warehouse database (clean data storage)
WAREHOUSE_DB_HOST=warehouse_postgres
WAREHOUSE_DB_PORT=5432
WAREHOUSE_DB_NAME=warehouse
WAREHOUSE_DB_USER=warehouse_user
WAREHOUSE_DB_PASSWORD=warehouse_pass
```

Replace `your_api_key_here` with your actual API key.

The `.gitignore` file already excludes `.env` from Git. Never paste your API key
anywhere that gets committed to the repository.

---

## Step 3: Start Docker

Open a terminal in the `retailco-pipeline` folder and run:

```bash
docker compose up -d
```

This starts the full infrastructure:

| Container | Purpose |
|---|---|
| `lake_postgres` | Raw lake database where API data is initially stored (port 5433) |
| `warehouse_postgres` | Clean warehouse where dlt loads normalized data (port 5435) |
| `airflow_postgres` | Airflow internal database — not your data (port 5434) |
| `airflow_init` | One-time setup container, runs once then exits |
| `airflow_createuser` | Creates the Airflow admin user, runs once then exits |
| `airflow_scheduler` | Runs DAGs on schedule |
| `airflow_webserver` | Airflow UI at http://localhost:8080 |

The first time you run this it will download Docker images which takes 3 to 5 minutes.
Subsequent starts are much faster.

Confirm everything started:

```bash
docker compose ps
```

You should see `lake_postgres`, `warehouse_postgres`, and both Airflow containers
showing as **healthy** or **running**.

---

## Step 4: Open Airflow

Go to `http://localhost:8080` and log in with:

- **Username:** `admin`
- **Password:** `admin`

---

## Step 5: Run the extract DAG

1. Find the DAG called **`erp_extract`** in the list
2. Click the toggle to enable it (turns blue)
3. Click the play button to trigger a manual run
4. Click the DAG name and select the **Graph** tab to watch tasks run in real time

The DAG has 9 tasks, one per entity. `order_items` and `inventory_movements` are large
and take longer on the first run (up to 2 hours each).

---

## Step 6: Run the dlt load pipeline

Once extraction is complete:

1. Find the DAG called **`load_warehouse`** in the Airflow list
2. Enable and trigger it
3. This runs the dlt pipeline which moves data from the lake to the warehouse,
   converts camelCase column names to snake_case, and applies correct data types

To run the dlt pipeline manually outside Airflow:

```bash
cd retailco-pipeline
python dlt_pipeline/load_warehouse.py
```

The pipeline uses incremental loading — on the first run it moves all rows,
on subsequent runs it only moves rows with a newer `updatedAt` timestamp.

---

## Step 7: Run dbt transformations

```bash
cd retailco_dbt
dbt debug                        # test connection
dbt snapshot                     # run SCD2 snapshots for dim_customer and dim_product
dbt run --select staging         # build all 9 staging models + flagged_payments
dbt run --select marts           # build dimensions and fact tables
dbt test                         # run all tests
dbt docs generate                # generate documentation site
dbt docs serve                   # open docs in browser
```

---

## Connecting to the databases directly

You can connect from any SQL client using these credentials:

### Lake database (raw data)

| Setting | Value |
|---|---|
| Host | localhost |
| Port | 5433 |
| Database | lake |
| Username | lake_user |
| Password | lake_pass |
| Schema | raw |

### Warehouse database (clean data)

| Setting | Value |
|---|---|
| Host | localhost |
| Port | 5435 |
| Database | warehouse |
| Username | warehouse_user |
| Password | warehouse_pass |
| Schema | raw (after dlt load), then marts (after dbt run) |

---

## Table reference

### Lake tables (`raw` schema, camelCase columns)

| Table | Description | Approx rows |
|---|---|---|
| `raw.customers` | One row per customer | ~5,000 |
| `raw.products` | One row per product | ~2,000 |
| `raw.stores` | Lagos, Abuja, Port Harcourt, Kano | ~4 |
| `raw.employees` | One row per employee | ~50 |
| `raw.orders` | One row per order | ~80,000 |
| `raw.order_items` | One row per order line item | ~360,000 |
| `raw.payments` | One row per payment event | ~72,000 |
| `raw.inventory_movements` | One row per stock movement | ~355,000 |
| `raw.payment_methods` | Cash, Card, Bank Transfer etc. | ~5 |
| `raw._watermarks` | Internal incremental tracking, 1 row per entity | 9 |

All lake columns are stored as `TEXT` or `JSONB` in camelCase as returned by the API
(e.g. `updatedAt`, `isDeleted`, `firstName`).

### Warehouse tables (after dlt load, snake_case columns)

The dlt pipeline converts all camelCase column names to snake_case and applies correct
data types automatically. For example `updatedAt TEXT` becomes `updated_at TIMESTAMPTZ`.

---

## How incremental loading works

### Extract layer (lake)

After the first full extract, every subsequent daily run only downloads records that changed
since the last run. This is tracked using `raw._watermarks` which stores the last
`updated_at` timestamp per entity. The extractor passes this as `?updated_after=<timestamp>`
to the API.

### Load layer (warehouse)

The dlt pipeline reads the `updatedAt` column from each lake table and only moves rows
with a newer timestamp than the last run. It uses `write_disposition="merge"` with `id`
as the primary key, so running the pipeline twice never creates duplicate rows.

---

## Flagged payments

Some payment rows are anomalous and excluded from revenue analysis:

- `amount_paid = 0` — always flagged as `zero_amount`
- `amount_paid < 0` and `payment_type != 'refund'` — flagged as `unexplained_negative`

Legitimate refunds (`amount_paid < 0` and `payment_type = 'refund'`) are valid records
and remain in the main payments flow.

Flagged rows are isolated into the `flagged_payments` table built by dbt. Run
`dbt run --select flagged_payments` to build it, then query it directly to see
what was isolated and why.

---

## Troubleshooting

**http://localhost:8080 will not open**
Airflow is still starting. Wait 2 to 3 minutes after `docker compose up -d` and try again.

**A task is red in Airflow**
Click the red task, click Logs, read the error. Common causes are API timeouts or rate
limiting — the extractor handles these with automatic retries. Click "Clear task" to retry.

**Tables are missing after a green run**
The entity returned zero rows — normal for incremental runs when nothing changed.
To force a full re-extract, delete the relevant row from `raw._watermarks` and re-run.

**Docker containers will not start**
Make sure Docker Desktop is open and the engine is running. Then run:

```bash
docker compose down -v
docker compose up -d
```

**dbt cannot connect to the warehouse**
Run `dbt debug` and check the error. Confirm `warehouse_postgres` is running with
`docker compose ps` and verify your `profiles.yml` matches the warehouse credentials.
