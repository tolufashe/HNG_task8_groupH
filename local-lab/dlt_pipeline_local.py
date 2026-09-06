"""
Stands in for the original `dlt_pipeline/dlt_pipeline.py`.

Same job as the original: move data from the "lake" into the "warehouse"
incrementally, using dlt's merge write disposition keyed on `id` /
`updated_at`. The only thing that changed is the storage engine — DuckDB
files instead of two Postgres containers — because Docker isn't available
here. The dbt project downstream doesn't care either way.

Run:
    .venv/Scripts/python.exe dlt_pipeline_local.py
"""

import duckdb
import dlt

LAKE_DB = "lake.duckdb"
WAREHOUSE_DB = "warehouse.duckdb"

ENTITIES = [
    "customers", "products", "stores", "employees", "orders",
    "order_items", "payments", "inventory_movements", "payment_methods",
]


def make_resource(entity):
    @dlt.resource(
        name=entity,
        write_disposition="merge",
        primary_key="id",
    )
    def resource(updated_at=dlt.sources.incremental("updated_at", initial_value="1900-01-01")):
        con = duckdb.connect(LAKE_DB, read_only=True)
        cursor = con.execute(
            f'SELECT * FROM raw."{entity}" WHERE updated_at > ? ORDER BY updated_at',
            [updated_at.last_value],
        )
        columns = [c[0] for c in cursor.description]
        for row in cursor.fetchall():
            yield dict(zip(columns, row))
        con.close()

    return resource


def run_dlt_pipeline():
    print("Starting dlt pipeline: lake.duckdb -> warehouse.duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="lake_to_warehouse_local",
        destination=dlt.destinations.duckdb(WAREHOUSE_DB),
        dataset_name="raw",
    )

    resources = [make_resource(entity) for entity in ENTITIES]
    load_info = pipeline.run(resources)

    print(load_info)


if __name__ == "__main__":
    run_dlt_pipeline()
