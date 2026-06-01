# RetailCo Data Pipeline

A production-grade data pipeline for RetailCo, a Nigerian retail chain with stores in Lagos, Abuja, Port Harcourt, and Kano. The pipeline extracts data from a live ERP API, loads it into a data warehouse, and transforms it into analytics-ready Kimball dimensional models, orchestrated end to end by Apache Airflow.

---

## Architecture

```
RetailCo ERP API (REST)
     │  HTTPS + X-API-Key
     ▼
Python Extractor  ──►  Lake Postgres (port 5433)
                              │  dlt incremental load
                              ▼
                       Warehouse Postgres (port 5435)
                              │  dbt transforms
                              ▼
                    Dimensions + Fact Tables (raw_marts schema)

Apache Airflow orchestrates all three stages daily.
```

**Tools:**
- Orchestration: Apache Airflow 2.9
- Extraction: Python 3.11
- Lake storage: PostgreSQL 15
- Loading: dlt (latest)
- Warehouse storage: PostgreSQL 15
- Transformation: dbt-core 1.7 + dbt-postgres
- Containerisation: Docker + Docker Compose

---

## Design artifacts

The Kimball bus matrix, warehouse ERD, and architecture diagram are in the `/design` folder of this repository.

**Bus matrix summary:**

| Fact table | Grain | dim_date | dim_customer | dim_product | dim_store | dim_employee | dim_payment_method |
|---|---|---|---|---|---|---|---|
| fct_sales | One row per order line | ✓ | ✓ | ✓ | ✓ | ✓ | - |
| fct_payments | One row per payment | ✓ | ✓ | - | ✓ | - | ✓ |
| fct_inventory_daily | One row per product × store × day | ✓ | - | ✓ | ✓ | - | - |
| fct_order_lifecycle | One row per order | ✓ | ✓ | - | ✓ | ✓ | - |

`dim_customer` and `dim_product` use **SCD Type 2**, they track history with `valid_from`, `valid_to`, and `is_current` columns.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [VS Code](https://code.visualstudio.com)
- [Python 3.11+](https://www.python.org/downloads)

Confirm Docker is running, the whale icon should appear in your taskbar with status "Engine running".

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/tolufashe/HNG_task8_groupH.git
cd HNG_task8_groupH
```

### 2. Create the `.env` file

Create `.env` in the root folder. This file holds secrets, **never commit it to Git**.

```
ERP_API_KEY=your_api_key_here
ERP_API_BASE_URL=your_api_url_here

LAKE_DB_HOST=localhost
LAKE_DB_PORT=5433
LAKE_DB_NAME=lake
LAKE_DB_USER=lake_user
LAKE_DB_PASSWORD=lake_pass

WAREHOUSE_DB_HOST=localhost
WAREHOUSE_DB_PORT=5435
WAREHOUSE_DB_NAME=warehouse
WAREHOUSE_DB_USER=warehouse_user
WAREHOUSE_DB_PASSWORD=warehouse_pass
```

### 3. Start Docker

```bash
docker compose up -d
```

This starts all containers:

| Container | Purpose | Port |
|---|---|---|
| `lake_postgres` | Raw lake database | 5433 |
| `warehouse_postgres` | Clean warehouse | 5435 |
| `airflow_postgres` | Airflow internal database | - |
| `airflow_scheduler` | Runs DAGs on schedule | - |
| `airflow_webserver` | Airflow UI | 8080 |

The first run downloads Docker images and installs dbt/dlt inside the Airflow container, allow 10 to 15 minutes.

Confirm everything is running:

```bash
docker compose ps
```

All containers should show as **healthy** or **running**.

---

## Running the pipeline

### Open Airflow

Go to `http://localhost:8080` and log in:
- Username: `admin`
- Password: `admin`

You will see three DAGs:

| DAG | Owner | What it does |
|---|---|---|
| `erp_extract` | person_b | Extracts all 9 entities from the ERP API into the lake |
| `dlt_load_warehouse` | person_c | Loads lake data into the warehouse via dlt |
| `dbt_transform` | person_d | Runs snapshots, staging, marts, and tests |

### Running order

The DAGs are chained, each waits for the previous to succeed before starting:

```
erp_extract  →  dlt_load_warehouse  →  dbt_transform
```

To trigger the full pipeline manually:

1. Enable and trigger `erp_extract`
2. Once it completes, enable and trigger `dlt_load_warehouse`
3. Once it completes, `dbt_transform` will run automatically

On subsequent days the entire chain runs automatically at midnight UTC.

### Running dbt manually

```bash
cd retailco_dbt
dbt debug # test connection
dbt snapshot # SCD2 history for customers and products
dbt run --select staging # clean and type-cast raw columns
dbt run --select marts # build dimensions and facts
dbt test # run all 58 data quality tests
dbt docs generate && dbt docs serve # browse documentation
```

---

## Querying the warehouse

Connect with any SQL client using:

| Setting | Lake | Warehouse |
|---|---|---|
| Host | localhost | localhost |
| Port | 5433 | 5435 |
| Database | lake | warehouse |
| User | lake_user | warehouse_user |
| Password | lake_pass | warehouse_pass |

### Useful queries

**Revenue by store:**
```sql
SELECT
    ds.store_name,
    ds.city,
    SUM(fs.line_total)      AS total_revenue,
    COUNT(DISTINCT fs.order_id) AS total_orders
FROM raw_marts.fct_sales fs
JOIN raw_marts.dim_store ds ON fs.store_sk = ds.store_sk
JOIN raw_marts.dim_date  dd ON fs.date_key  = dd.date_key
GROUP BY ds.store_name, ds.city
ORDER BY total_revenue DESC;
```

**Monthly revenue trend:**
```sql
SELECT
    dd.year,
    dd.month,
    SUM(fs.line_total) AS revenue
FROM raw_marts.fct_sales fs
JOIN raw_marts.dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;
```

**Top 10 products by revenue:**
```sql
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.line_total)  AS revenue,
    SUM(fs.quantity)    AS units_sold
FROM raw_marts.fct_sales fs
JOIN raw_marts.dim_product dp ON fs.product_sk = dp.product_sk
WHERE dp.is_current = true
GROUP BY dp.product_name, dp.category
ORDER BY revenue DESC
LIMIT 10;
```

**Customer segments by order value:**
```sql
SELECT
    dc.segment,
    dc.tier,
    COUNT(DISTINCT fo.order_id)  AS order_count,
    AVG(fo.total_amount)         AS avg_order_value
FROM raw_marts.fct_order_lifecycle fo
JOIN raw_marts.dim_customer dc ON fo.customer_sk = dc.customer_sk
WHERE dc.is_current = true
GROUP BY dc.segment, dc.tier
ORDER BY avg_order_value DESC;
```

**Payment method breakdown:**
```sql
SELECT
    dpm.payment_method_name,
    COUNT(*)               AS payment_count,
    SUM(fp.amount_paid)    AS total_amount
FROM raw_marts.fct_payments fp
JOIN raw_marts.dim_payment_method dpm
    ON fp.payment_method_sk = dpm.payment_method_sk
GROUP BY dpm.payment_method_name
ORDER BY payment_count DESC;
```

**Flagged anomalous payments:**
```sql
SELECT
    flag_reason,
    COUNT(*)            AS count,
    SUM(amount_paid)    AS total_amount
FROM raw_marts.flagged_payments
GROUP BY flag_reason
ORDER BY count DESC;
```

**Current inventory by store and product:**
```sql
SELECT
    ds.store_name,
    dp.product_name,
    dp.category,
    fi.closing_balance
FROM raw_marts.fct_inventory_daily fi
JOIN raw_marts.dim_store   ds ON fi.store_sk   = ds.store_sk
JOIN raw_marts.dim_product dp ON fi.product_sk = dp.product_sk
WHERE fi.date_key = (SELECT MAX(date_key) FROM raw_marts.fct_inventory_daily)
ORDER BY fi.closing_balance DESC;
```

---

## Table reference

### Lake (`raw` schema, camelCase columns, all TEXT)

| Table | Rows | Description |
|---|---|---|
| `raw.customers` | 5,000 | Customer records |
| `raw.products` | 2,000 | Product catalogue |
| `raw.stores` | 4 | Lagos, Abuja, Port Harcourt, Kano |
| `raw.employees` | 50 | Staff records |
| `raw.orders` | 80,000 | Order headers |
| `raw.order_items` | 360,783 | Order line items |
| `raw.payments` | 72,076 | Payment events (includes refunds) |
| `raw.inventory_movements` | 355,726 | Stock in/out events |
| `raw.payment_methods` | 5 | Cash, Card, Transfer, etc. |
| `raw._watermarks` | 9 | Incremental load tracking |

### Warehouse dimensions (`raw_marts` schema)

| Table | Rows | Notes |
|---|---|---|
| `dim_customer` | 5,000 | SCD2,  history tracked via valid_from/valid_to |
| `dim_product` | 2,000 | SCD2, history tracked via valid_from/valid_to |
| `dim_store` | 4 | Static |
| `dim_employee` | 50 | Static |
| `dim_date` | 2,192 | includes Nigerian public holidays |
| `dim_payment_method` | 5 | Static |

### Warehouse facts (`raw_marts` schema)

| Table | Rows | Grain |
|---|---|---|
| `fct_sales` | 342,926 | One row per order line item |
| `fct_payments` | 70,641 | One row per payment event |
| `fct_inventory_daily` | 344,905 | One row per product × store × day |
| `fct_order_lifecycle` | 80,000 | One row per order |
| `flagged_payments` | 1,435 | Anomalous payments excluded from fct_payments |

---

## How incremental loading works

**Extract (lake):** After the first full extract, daily runs only download records updated since the last run. The `raw._watermarks` table stores the last `updatedAt` timestamp per entity. The extractor passes `?updated_after=<timestamp>` to the API.

**Load (warehouse):** dlt reads `updatedAt` from each lake table and only moves new or updated rows. `write_disposition="merge"` with `id` as the primary key prevents duplicates.

**Transform (dbt):** dbt snapshots detect row-level changes using the `updated_at` column and insert new history records — never overwriting old ones. This preserves the full change history for customers and products.

---

## Data quality

58 automated dbt tests run after every pipeline execution:

- `not_null`: all primary and foreign keys
- `unique`: all surrogate keys
- `relationships`: all foreign key references to dimension tables
- Custom business logic:
  - No negative quantities in `fct_inventory_daily`
  - Orders delivered after they were placed in `fct_order_lifecycle`
  - No flagged payments leaking into `fct_payments`
  - No negative line totals in `fct_sales`

---

## Troubleshooting

**`http://localhost:8080` will not open**
Airflow is still installing packages. Wait 10–15 minutes after `docker compose up -d` then try again.

**A task is red in Airflow**
Click the task → Logs → read the error. API timeouts are handled automatically with retries. Click "Clear task" to retry a failed task.

**Tables are missing after a green run**
The entity returned zero rows, normal for incremental runs when nothing changed. To force a full re-extract, delete the relevant row from `raw._watermarks` and re-run.

**dbt cannot connect**
Run `dbt debug` inside `retailco_dbt/`. Confirm `warehouse_postgres` is running with `docker compose ps` and verify `retailco_dbt/profiles.yml` matches warehouse credentials.

**Containers will not start**
```bash
docker compose down -v
docker compose up -d
```

---

## Repository structure

```
HNG_task8_groupH/
├── design/                        # CP1: bus matrix, ERD, architecture diagram
├── extractor/                     # CP2: Python ERP extractor
│   ├── erp_extractor.py
│   └── dags/
│       ├── extract_dag.py         # erp_extract DAG
│       ├── load_dag.py            # dlt_load_warehouse DAG
│       └── dbt_dag.py             # dbt_transform DAG
├── dlt_pipeline/                  # CP3: dlt lake-to-warehouse loader
│   └── dlt_pipeline.py
├── retailco_dbt/                  # CP4: dbt project
│   ├── models/
│   │   ├── staging/               # 9 staging views
│   │   └── marts/
│   │       ├── dimensions/        # 6 dimension tables
│   │       └── facts/             # 4 fact tables + flagged_payments
│   ├── snapshots/                 # SCD2 snapshots
│   ├── tests/                     # Custom data tests
│   ├── profiles.yml
│   └── dbt_project.yml
├── docker-compose.yml             # CP5: full infrastructure
├── .env.example                   # Template: copy to .env and fill in secrets
├── .gitignore
└── README.md
```

---

## Team

| Person | Role | Checkpoints |
|---|---|---|
| Person A (Miss Cryptic) | Design + Orchestration | CP1, CP5 |
| Person B (KB) [TL]| Extraction | CP2 |
| Person C (Tolufashe)| Loading | CP3 |
| Person D (AnalystMund)| Modelling | CP4 |
