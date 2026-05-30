# ERP Extraction 

This section covers everything you need to set up and run the ERP data extractor.
The extractor pulls data from the RetailCo ERP API and stores it in a raw lake
PostgreSQL database, ready to load into the warehouse.

---

## What this does

The extractor connects to the RetailCo ERP REST API, downloads all 9 business
entities, and writes them into a PostgreSQL database called the **raw lake**.
It runs automatically every day via Apache Airflow.

On the first run it downloads everything. On every run after that it only
downloads records that are new or updated since the last run, so it stays
fast and efficient.

---

## Prerequisites

Before running anything, make sure you have the following installed on your machine:

- **Docker Desktop**: download from https://www.docker.com/products/docker-desktop
- **VS Code**: download from https://code.visualstudio.com
- **Python 3.11+**: download from https://www.python.org/downloads

Verify Docker is running by opening Docker Desktop and confirming the whale
icon appears in your taskbar with the status "Engine running".

---

## Step 1: Clone the repository

```bash
git clone <your-team-repo-url>
cd retailco-pipeline
```

---

## Step 2: Configure the `.env` file

Create a file called `.env` in the root `retailco-pipeline` folder.
This file holds all secrets and connection details. **Never commit this file to Git.**

```
# ERP API credentials
ERP_API_KEY=your_api_key_here

# Lake database (raw data storage)
LAKE_DB_HOST=lake_postgres
LAKE_DB_PORT=5432
LAKE_DB_NAME=lake
LAKE_DB_USER=lake_user
LAKE_DB_PASSWORD=lake_pass
```

Replace `your_api_key_here` with your actual API key.

> **Important:** The `.gitignore` file already excludes `.env` from Git.
> Never paste your API key anywhere that gets committed to the repository.

---

## Step 3: Start Docker

Open a terminal in the `retailco-pipeline` folder and run:

```bash
docker compose up -d
```

This command starts four containers:

| Container | Purpose |
|---|---|
| `lake_postgres` | The raw lake database where extracted data is stored |
| `airflow_postgres` | Airflow's internal database (not your data) |
| `airflow_init` | One-time setup container, runs once then exits |
| `airflow_createuser` | Creates the Airflow admin user, runs once then exits |
| `airflow_scheduler` | Runs DAGs on schedule |
| `airflow_webserver` | The Airflow UI accessible at http://localhost:8080 |

The first time you run this it will download the Docker images which takes
3 to 5 minutes. Subsequent starts are much faster.

To confirm everything started correctly:

```bash
docker compose ps
```

You should see `lake_postgres` and both Airflow containers showing as
**healthy** or **running**. The `airflow_init` and `airflow_createuser`
containers will show as **exited**, this is correct, they are one-time
setup containers.

---

## Step 4: Open Airflow

Open your browser and go to:

```
http://localhost:8080
```

Log in with:
- **Username:** `admin`
- **Password:** `admin`

You will see the Airflow DAGs page.

---

## Step 5: Run the extract DAG

1. Find the DAG called **`erp_extract`** in the list
2. Click the **toggle** on the left to enable it (it turns blue)
3. Click the **play button** on the right to trigger a manual run
4. Click on the DAG name to open it
5. Click the **Graph** tab to watch tasks run in real time

The DAG has 9 tasks, one for each entity. They will turn deep green one by one
as each entity finishes extracting. Most tasks complete in a few minutes.
`order_items` and `inventory_movements` are large datasets and will take
longer on the first run (up to 2 hours each on a full extract).

---

## Step 6: Verify data landed

Once all tasks are deep green, connect to the lake database and confirm the
tables were created:

```bash
docker exec -it lake_postgres psql -U lake_user -d lake -c \
"SELECT relname as table_name, n_live_tup as row_count \
FROM pg_stat_user_tables WHERE schemaname = 'raw' \
ORDER BY n_live_tup DESC;"
```

You should see 10 tables with row counts similar to these:

| Table | Expected rows |
|---|---|
| raw.order_items | ~360,000 |
| raw.inventory_movements | ~355,000 |
| raw.orders | ~80,000 |
| raw.payments | ~72,000 |
| raw.customers | ~5,000 |
| raw.products | ~2,000 |
| raw.employees | ~50 |
| raw.payment_methods | ~5 |
| raw.stores | ~4 |
| raw._watermarks | 9 (one per entity) |

---

## How incremental loading works

After the first full extract, every subsequent daily run only downloads
records that changed since the last run. This is tracked using a
**watermarks table**, `raw._watermarks`, which stores the timestamp
of the most recently updated record per entity.

To check current watermarks:

```bash
docker exec -it lake_postgres psql -U lake_user -d lake -c \
"SELECT * FROM raw._watermarks ORDER BY entity;"
```

On the next run, the extractor passes each watermark as
`?updated_after=<timestamp>` to the API, so only new or updated
records are returned.

---

## Table reference

All tables live in the `raw` schema of the `lake` database.
Every table has an `_extracted_at` column added by the extractor
showing when each row was last pulled from the API.
All columns are stored as `TEXT` or `JSONB` — type casting happens
in the dbt staging layer (Person D's responsibility).

Column names are in **camelCase** as returned by the API
(e.g. `updatedAt`, `isDeleted`, `firstName`). Person C's dlt pipeline
converts these to `snake_case` when loading into the warehouse.

### `raw.customers`
One row per customer. Key columns:
`id`, `firstName`, `lastName`, `email`, `phone`, `segment`, `tier`,
`address`, `city`, `state`, `effectiveFrom`, `registeredAt`, `isDeleted`,
`createdAt`, `updatedAt`

### `raw.products`
One row per product. Key columns:
`id`, `name`, `category`, `price`, `cost`, `isDeleted`, `updatedAt`

### `raw.stores`
One row per RetailCo store (Lagos, Abuja, Port Harcourt, Kano). Key columns:
`id`, `name`, `city`, `state`, `updatedAt`

### `raw.employees`
One row per employee. Key columns:
`id`, `firstName`, `lastName`, `role`, `storeId`, `updatedAt`

### `raw.orders`
One row per customer order. Key columns:
`id`, `customerId`, `storeId`, `status`, `createdAt`, `updatedAt`
Status progresses through: `pending → paid → shipped → delivered`

### `raw.order_items`
One row per line item within an order (~360,000 rows). Key columns:
`id`, `orderId`, `productId`, `quantity`, `unitPrice`, `discount`, `updatedAt`

### `raw.payments`
One row per payment event. Key columns:
`id`, `orderId`, `customerId`, `amountPaid`, `paymentMethodId`,
`paymentDate`, `updatedAt`
Note: negative `amountPaid` values are valid refund records.
Zero or unexplained negative amounts are flagged in Person D's
`flagged_payments` table.

### `raw.inventory_movements`
One row per stock movement event (~355,000 rows). Key columns:
`id`, `productId`, `storeId`, `movementType`, `quantity`,
`movementDate`, `updatedAt`

### `raw.payment_methods`
One row per payment method (card, transfer, cash, etc.). Key columns:
`id`, `name`, `updatedAt`

### `raw._watermarks`
Internal tracking table — not source data. One row per entity.
Stores the `last_updated_at` timestamp used for incremental loading.

---

## Connecting to the lake database directly

You can connect to the lake database from any SQL client using:

| Setting | Value |
|---|---|
| Host | localhost |
| Port | 5433 |
| Database | lake |
| Username | lake_user |
| Password | lake_pass |
| Schema | raw |


---

## Troubleshooting

**http://localhost:8080 won't open**
Airflow is still starting up. Wait 2–3 minutes after running
`docker compose up -d` and try again.

**A task is red in Airflow**
Click on the red task → click Logs → read the error message.
Common causes: API timeout (the ERP occasionally has slow responses),
rate limiting (the extractor handles this automatically with retries).
Click "Clear task" to retry.

**Tables are missing after a green run**
The entity returned zero rows from the API on that run. This is normal
for incremental runs when no data has changed. Trigger a full re-extract
by deleting the relevant row from `raw._watermarks` and re-running.

**Docker containers won't start**
Make sure Docker Desktop is open and the engine is running (whale icon
in taskbar). Then run `docker compose down -v` and `docker compose up -d`
to start fresh.

---
