import os
import dlt
from dlt.sources.sql_database import sql_database
from dotenv import load_dotenv
 
# Load environment variables from the .env file
load_dotenv()
 
# List of ERP entities to be extracted from the data lake
ENTITIES = [
    "customers", "products", "stores", "employees", "orders",
    "order_items", "payments", "inventory_movements", "payment_methods"
]
 
def run_dlt_pipeline():
    """
    Extracts raw data from the Lake PostgreSQL database and loads it incrementally
    into the Warehouse PostgreSQL database. Designed to be callable by Airflow tasks.
    """
    print("Starting dlt pipeline: Lake -> Warehouse")
 
    # 1. Grab credentials from the environment (consistently using _DB_ naming)
    lake_user = os.getenv("LAKE_DB_USER")
    lake_pass = os.getenv("LAKE_DB_PASSWORD")
    lake_host = os.getenv("LAKE_DB_HOST")
    lake_port = os.getenv("LAKE_DB_PORT")
    lake_name = os.getenv("LAKE_DB_NAME")
 
    wh_user = os.getenv("WAREHOUSE_DB_USER")
    wh_pass = os.getenv("WAREHOUSE_DB_PASSWORD")
    wh_host = os.getenv("WAREHOUSE_DB_HOST")
    wh_port = os.getenv("WAREHOUSE_DB_PORT")
    wh_name = os.getenv("WAREHOUSE_DB_NAME")
 
    # 2. Build the connection strings
    source_url = f"postgresql://{lake_user}:{lake_pass}@{lake_host}:{lake_port}/{lake_name}"
    dest_url = f"postgresql://{wh_user}:{wh_pass}@{wh_host}:{wh_port}/{wh_name}"
 
    # 3. Configure the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="lake_to_warehouse",
        destination=dlt.destinations.postgres(dest_url),
        dataset_name="raw",  # The schema in the warehouse where data will land
    )
 
    # 4. Tell dlt to pull the specific entities from the 'raw' schema in the Lake
    source = sql_database(
        credentials=source_url,
        schema="raw"
    ).with_resources(*ENTITIES)
 
    # Apply incremental loading hints to each entity so only new or updated rows move.
    # Uses camelCase column names because the Lake stores data exactly as the API returned it.
    for entity in ENTITIES:
        if entity in source.resources:
            source.resources[entity].apply_hints(
                primary_key="id",
                incremental=dlt.sources.incremental("updatedAt")  # camelCase to match Lake columns
            )
 
    # 5. Run the pipeline (merge handles new and updated rows automatically)
    load_info = pipeline.run(source, write_disposition="merge")
 
    print(load_info)
 
if __name__ == "__main__":
    run_dlt_pipeline()