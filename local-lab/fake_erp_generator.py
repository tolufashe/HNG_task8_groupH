"""
Stands in for the original `extractor/erp_extractor.py`.

The real project pulled data from a live ERP REST API
(https://hngstage8da-...herokuapp.com) that no longer exists. Instead of
calling a dead API, this script *generates* realistic-looking RetailCo data
locally and writes it straight into a DuckDB file that plays the role of the
"lake" database (`raw` schema), so the rest of the pipeline (dlt load -> dbt
transform) can run unmodified against it.

Column names deliberately match what retailco_dbt/models/staging/*.sql
already expects, so those models can be reused with zero changes.

Run:
    .venv/Scripts/python.exe fake_erp_generator.py
"""

import random
import uuid
from datetime import datetime, timedelta

import duckdb
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

LAKE_DB = "lake.duckdb"

N_CUSTOMERS = 500
N_PRODUCTS = 200
N_STORES = 4
N_EMPLOYEES = 20
N_ORDERS = 4000

STORES = [
    {"name": "RetailCo Lagos", "city": "Lagos", "state": "Lagos"},
    {"name": "RetailCo Abuja", "city": "Abuja", "state": "FCT"},
    {"name": "RetailCo Port Harcourt", "city": "Port Harcourt", "state": "Rivers"},
    {"name": "RetailCo Kano", "city": "Kano", "state": "Kano"},
]

CATEGORIES = {
    "Groceries": ["Snacks", "Beverages", "Staples"],
    "Electronics": ["Phones", "Accessories", "Home Appliances"],
    "Fashion": ["Menswear", "Womenswear", "Footwear"],
    "Home & Living": ["Kitchenware", "Furniture", "Decor"],
    "Beauty": ["Skincare", "Haircare", "Fragrance"],
}

SEGMENTS = ["Retail", "Wholesale", "VIP"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
ORDER_STATUSES = ["delivered", "shipped", "cancelled", "pending"]
PAYMENT_METHODS = [
    {"name": "Cash", "provider": "N/A", "is_digital": False},
    {"name": "Card", "provider": "Paystack", "is_digital": True},
    {"name": "Bank Transfer", "provider": "Flutterwave", "is_digital": True},
    {"name": "USSD", "provider": "Interswitch", "is_digital": True},
    {"name": "Wallet", "provider": "OPay", "is_digital": True},
]
ROLES = ["Cashier", "Store Manager", "Sales Associate", "Stock Clerk"]
MOVEMENT_TYPES = ["restock", "sale", "adjustment", "return"]

now = datetime.utcnow()
START = now - timedelta(days=365 * 2)


def rand_ts(start=START, end=now):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def new_id():
    return str(uuid.uuid4())


def gen_stores():
    rows = []
    for s in STORES:
        created = rand_ts(START, START + timedelta(days=30))
        rows.append({
            "id": new_id(),
            "name": s["name"],
            "city": s["city"],
            "state": s["state"],
            "address": fake.street_address(),
            "phone": fake.phone_number(),
            "manager_name": fake.name(),
            "opened_date": created.date().isoformat(),
            "created_at": created.isoformat(),
            "updated_at": created.isoformat(),
        })
    return rows


def gen_employees(store_ids):
    rows = []
    for _ in range(N_EMPLOYEES):
        created = rand_ts()
        rows.append({
            "id": new_id(),
            "store_id": random.choice(store_ids),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "role": random.choice(ROLES),
            "hired_date": created.date().isoformat(),
            "is_deleted": random.random() < 0.03,
            "created_at": created.isoformat(),
            "updated_at": created.isoformat(),
        })
    return rows


def gen_payment_methods():
    rows = []
    for pm in PAYMENT_METHODS:
        created = rand_ts(START, START + timedelta(days=1))
        rows.append({
            "id": new_id(),
            "name": pm["name"],
            "provider": pm["provider"],
            "is_digital": pm["is_digital"],
            "created_at": created.isoformat(),
            "updated_at": created.isoformat(),
        })
    return rows


def gen_customers():
    rows = []
    for _ in range(N_CUSTOMERS):
        created = rand_ts()
        updated = rand_ts(created, now)
        rows.append({
            "id": new_id(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "segment": random.choice(SEGMENTS),
            "tier": random.choice(TIERS),
            "address": fake.street_address(),
            "city": random.choice(STORES)["city"],
            "state": random.choice(STORES)["state"],
            "effective_from": created.isoformat(),
            "registered_at": created.isoformat(),
            "is_deleted": random.random() < 0.02,
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
        })
    return rows


def gen_products():
    rows = []
    for _ in range(N_PRODUCTS):
        category = random.choice(list(CATEGORIES.keys()))
        sub_category = random.choice(CATEGORIES[category])
        cost = round(random.uniform(500, 50000), 2)
        margin = random.uniform(1.15, 1.8)
        created = rand_ts()
        updated = rand_ts(created, now)
        rows.append({
            "id": new_id(),
            "sku": fake.unique.bothify(text="SKU-#####??"),
            "name": fake.catch_phrase(),
            "category": category,
            "sub_category": sub_category,
            "brand": fake.company(),
            "supplier": fake.company(),
            "cost_price": cost,
            "selling_price": round(cost * margin, 2),
            "effective_from": created.isoformat(),
            "is_deleted": random.random() < 0.02,
            "created_at": created.isoformat(),
            "updated_at": updated.isoformat(),
        })
    return rows


def gen_orders_and_items(customer_ids, store_ids, employee_ids, products):
    orders, items, payments = [], [], []
    for _ in range(N_ORDERS):
        ordered_at = rand_ts()
        status = random.choices(
            ORDER_STATUSES, weights=[0.7, 0.15, 0.1, 0.05]
        )[0]

        order_id = new_id()
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        employee_id = random.choice(employee_ids)

        paid_at = ordered_at + timedelta(minutes=random.randint(1, 60)) if status != "pending" else None
        shipped_at = paid_at + timedelta(hours=random.randint(1, 48)) if paid_at and status in ("shipped", "delivered") else None
        delivered_at = shipped_at + timedelta(days=random.randint(1, 5)) if shipped_at and status == "delivered" else None
        cancelled_at = ordered_at + timedelta(hours=random.randint(1, 24)) if status == "cancelled" else None

        n_lines = random.randint(1, 5)
        chosen_products = random.sample(products, n_lines)
        total_amount = 0
        for p in chosen_products:
            quantity = random.randint(1, 4)
            discount_pct = random.choice([0, 0, 0, 5, 10, 15, 20])
            unit_price = p["selling_price"]
            line_total = round(unit_price * quantity * (1 - discount_pct / 100), 2)
            total_amount += line_total
            items.append({
                "id": new_id(),
                "order_id": order_id,
                "product_id": p["id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "line_total": line_total,
                "created_at": ordered_at.isoformat(),
                "updated_at": ordered_at.isoformat(),
            })

        discount_code = random.choice([None, None, None, "WELCOME10", "SAVE20"])
        discount_amount = round(total_amount * 0.1, 2) if discount_code else 0

        orders.append({
            "id": order_id,
            "customer_id": customer_id,
            "store_id": store_id,
            "employee_id": employee_id,
            "status": status,
            "discount_code": discount_code,
            "discount_amount": discount_amount,
            "total_amount": round(total_amount - discount_amount, 2),
            "ordered_at": ordered_at.isoformat(),
            "paid_at": paid_at.isoformat() if paid_at else None,
            "shipped_at": shipped_at.isoformat() if shipped_at else None,
            "delivered_at": delivered_at.isoformat() if delivered_at else None,
            "cancelled_at": cancelled_at.isoformat() if cancelled_at else None,
            "created_at": ordered_at.isoformat(),
            "updated_at": (delivered_at or cancelled_at or paid_at or ordered_at).isoformat(),
        })

        if paid_at:
            is_refund = random.random() < 0.03
            payments.append({
                "id": new_id(),
                "order_id": order_id,
                "customer_id": customer_id,
                "payment_method_id": None,  # filled in after generation
                "amount_paid": -round(total_amount * 0.5, 2) if is_refund else round(total_amount - discount_amount, 2),
                "currency": "NGN",
                "status": "refunded" if is_refund else "completed",
                "payment_type": "refund" if is_refund else "sale",
                "reference": fake.unique.bothify(text="PSK-##########"),
                "paid_at": paid_at.isoformat(),
                "created_at": paid_at.isoformat(),
                "updated_at": paid_at.isoformat(),
            })

    return orders, items, payments


def gen_inventory_movements(products, store_ids):
    rows = []
    for p in products:
        for store_id in store_ids:
            for _ in range(random.randint(2, 6)):
                moved_at = rand_ts()
                movement_type = random.choice(MOVEMENT_TYPES)
                quantity = random.randint(1, 100) if movement_type == "restock" else -random.randint(1, 20)
                rows.append({
                    "id": new_id(),
                    "product_id": p["id"],
                    "store_id": store_id,
                    "movement_type": movement_type,
                    "quantity": quantity,
                    "reference_id": new_id(),
                    "reference_type": "order" if movement_type == "sale" else "manual",
                    "notes": fake.sentence(nb_words=6) if random.random() < 0.2 else None,
                    "moved_at": moved_at.isoformat(),
                    "created_at": moved_at.isoformat(),
                    "updated_at": moved_at.isoformat(),
                })
    return rows


def sql_type_for(column, rows):
    for row in rows:
        value = row[column]
        if value is None:
            continue
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "BIGINT"
        if isinstance(value, float):
            return "DOUBLE"
        return "VARCHAR"
    return "VARCHAR"


def load_table(con, table, rows):
    if not rows:
        return
    columns = list(rows[0].keys())
    col_defs = ", ".join(f'"{c}" {sql_type_for(c, rows)}' for c in columns)
    placeholders = ", ".join(["?"] * len(columns))

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f'DROP TABLE IF EXISTS raw."{table}"')
    con.execute(f'CREATE TABLE raw."{table}" ({col_defs})')

    values = [[row[c] for c in columns] for row in rows]
    con.executemany(f'INSERT INTO raw."{table}" VALUES ({placeholders})', values)
    print(f"[{table}] {len(rows)} rows -> raw.{table}")


def main():
    con = duckdb.connect(LAKE_DB)

    stores = gen_stores()
    employees = gen_employees([s["id"] for s in stores])
    payment_methods = gen_payment_methods()
    customers = gen_customers()
    products = gen_products()
    orders, order_items, payments = gen_orders_and_items(
        [c["id"] for c in customers],
        [s["id"] for s in stores],
        [e["id"] for e in employees],
        products,
    )
    # backfill payment_method_id now that payment_methods exist
    pm_ids = [pm["id"] for pm in payment_methods]
    for p in payments:
        p["payment_method_id"] = random.choice(pm_ids)

    inventory_movements = gen_inventory_movements(products, [s["id"] for s in stores])

    load_table(con, "stores", stores)
    load_table(con, "employees", employees)
    load_table(con, "payment_methods", payment_methods)
    load_table(con, "customers", customers)
    load_table(con, "products", products)
    load_table(con, "orders", orders)
    load_table(con, "order_items", order_items)
    load_table(con, "payments", payments)
    load_table(con, "inventory_movements", inventory_movements)

    con.close()
    print(f"\nDone. Lake written to {LAKE_DB}")


if __name__ == "__main__":
    main()
