import os
import time
import json
import logging
from datetime import datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# env and logging
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)

log = logging.getLogger(__name__)

# Config
API_BASE = "https://hngstage8da-55c7f5f769c8.herokuapp.com"
API_KEY = os.getenv("ERP_API_KEY")

MAX_RETRIES = 5
REQUEST_TIMEOUT = (10, 120)
PAGE_LIMIT = 100

DB_CONFIG = {
    "host": os.getenv("LAKE_DB_HOST", "localhost"),
    "port": int(os.getenv("LAKE_DB_PORT", 5432)),
    "dbname": os.getenv("LAKE_DB_NAME"),
    "user": os.getenv("LAKE_DB_USER"),
    "password": os.getenv("LAKE_DB_PASSWORD"),
}

ENTITIES = [
    "customers",
    "products",
    "stores",
    "employees",
    "orders",
    "order_items",
    "payments",
    "inventory_movements",
    "payment_methods",
]

# Http Session 
session = requests.Session()


def get_headers():
    return {
        "X-API-Key": API_KEY
    }


# Database
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# Retry and Http 
def fetch_with_retry(url, params=None):
    """
    GET request with:
    - retry handling
    - exponential backoff
    - rate-limit handling
    - timeout handling
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(
                url,
                headers=get_headers(),
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            # SUCCESS
            if response.status_code == 200:
                return response.json()

            # RATE LIMIT
            elif response.status_code == 429:
                wait = int(
                    response.headers.get(
                        "Retry-After",
                        2 ** attempt
                    )
                )

                log.warning(
                    f"429 Rate Limit → sleeping {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )

                time.sleep(wait)

            # TRANSIENT SERVER ERRORS
            elif response.status_code in [500, 502, 503, 504]:

                wait = 2 ** attempt

                log.warning(
                    f"{response.status_code} Server Error → "
                    f"sleeping {wait}s "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )

                time.sleep(wait)

            else:
                response.raise_for_status()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException
        ) as e:

            wait = 2 ** attempt

            log.warning(
                f"Request failed: {e} → "
                f"sleeping {wait}s "
                f"(attempt {attempt}/{MAX_RETRIES})"
            )

            time.sleep(wait)

    raise Exception(f"All retries failed for {url}")


# Watermarks
def ensure_watermark_table(conn):

    with conn.cursor() as cur:

        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw._watermarks (
                entity TEXT PRIMARY KEY,
                last_updated_at TIMESTAMPTZ
            );
        """)

    conn.commit()


def get_watermark(conn, entity):
    """
    Returns:
    - None on first run
    - watermark minus 1 day on incremental runs

    Why minus 1 day?
    To safely capture late-arriving updates.
    """

    ensure_watermark_table(conn)

    with conn.cursor() as cur:

        cur.execute("""
            SELECT last_updated_at
            FROM raw._watermarks
            WHERE entity = %s
        """, (entity,))

        row = cur.fetchone()

    if not row:
        return None

    watermark = row[0]

    # Sliding overlap window
    safe_watermark = watermark - timedelta(days=1)

    return safe_watermark.isoformat()


def save_watermark(conn, entity, rows):

    timestamps = [
        r.get("updatedAt")
        for r in rows
        if r.get("updatedAt")
    ]

    if not timestamps:
        log.warning(
            f"[{entity}] no updated_at values found"
        )
        return

    latest = max(timestamps)

    with conn.cursor() as cur:

        cur.execute("""
            INSERT INTO raw._watermarks (
                entity,
                last_updated_at
            )
            VALUES (%s, %s)

            ON CONFLICT (entity)
            DO UPDATE SET
                last_updated_at = EXCLUDED.last_updated_at;
        """, (entity, latest))

    conn.commit()

    log.info(
        f"[{entity}] watermark updated → {latest}"
    )


# Schema Management
def ensure_table(conn, entity, sample_row):
    """
    Creates table dynamically.
    Also handles schema evolution safely.
    """

    with conn.cursor() as cur:

        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
        """)

        # Create base table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS raw.{entity} (
                id TEXT PRIMARY KEY,
                _extracted_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # Add columns dynamically
        for column, value in sample_row.items():

            if column == "id":
                continue

            column_type = (
                "JSONB"
                if isinstance(value, (dict, list))
                else "TEXT"
            )

            cur.execute(f"""
                ALTER TABLE raw.{entity}
                ADD COLUMN IF NOT EXISTS "{column}" {column_type};
            """)

    conn.commit()


# Upserts
def upsert_rows(conn, entity, rows):

    if not rows:
        return

    columns = list(rows[0].keys())

    col_names = ", ".join(
        f'"{c}"'
        for c in columns
    )

    update_clause = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in columns
        if c != "id"
    )

    sql = f"""
        INSERT INTO raw.{entity} ({col_names})

        VALUES %s

        ON CONFLICT ("id")
        DO UPDATE SET
            {update_clause},
            _extracted_at = NOW();
    """

    values = []

    for row in rows:

        row_values = []

        for column in columns:

            value = row.get(column)

            if isinstance(value, (dict, list)):
                value = json.dumps(value)

            elif value is not None:
                value = str(value)

            row_values.append(value)

        values.append(tuple(row_values))

    with conn.cursor() as cur:

        execute_values(
            cur,
            sql,
            values,
            page_size=100
        )

    conn.commit()

    log.info(
        f"[{entity}] {len(rows)} rows upserted"
    )


# Pagination
def fetch_all_pages(entity, updated_after=None):

    url = f"{API_BASE}/{entity}"

    params = {
        "limit": PAGE_LIMIT
    }

    if updated_after:
        params["updated_after"] = updated_after

        log.info(
            f"[{entity}] incremental load "
            f"since {updated_after}"
        )

    else:
        log.info(
            f"[{entity}] full load"
        )

    all_rows = []
    page = 0

    while True:

        body = fetch_with_retry(url, params)

        rows = body.get("data", [])

        meta = body.get("meta", {})

        has_more = meta.get("has_more", False)

        cursor = meta.get("cursor")

        all_rows.extend(rows)

        page += 1

        log.info(
            f"[{entity}] page={page} "
            f"rows={len(rows)} "
            f"total={len(all_rows)} "
            f"has_more={has_more}"
        )

        if not has_more or not cursor:
            break

        params["cursor"] = cursor

    log.info(
        f"[{entity}] extraction complete "
        f"→ {len(all_rows)} rows"
    )

    return all_rows


# Main Extraction Flow
def extract_entity(entity):

    log.info("=" * 60)
    log.info(f"START EXTRACT → {entity}")

    conn = get_connection()

    try:

        watermark = get_watermark(conn, entity)

        rows = fetch_all_pages(
            entity,
            updated_after=watermark
        )

        if not rows:
            log.info(
                f"[{entity}] no new rows"
            )
            return

        ensure_table(
            conn,
            entity,
            rows[0]
        )

        upsert_rows(
            conn,
            entity,
            rows
        )

        save_watermark(
            conn,
            entity,
            rows
        )

    except Exception as e:

        log.error(
            f"[{entity}] extraction failed → {e}"
        )

        raise

    finally:
        conn.close()

    log.info(f"END EXTRACT → {entity}")


# Manual Runner
def extract_all():

    for entity in ENTITIES:

        extract_entity(entity)


if __name__ == "__main__":
    extract_all()
