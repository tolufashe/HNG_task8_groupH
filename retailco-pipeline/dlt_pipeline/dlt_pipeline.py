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
    
    # 1. Configure the Source Connection (Data Lake)
    # Construct the connection string using lake-specific environment variables
    lake_url = (
        f"postgresql://{os.getenv('LAKE_DB_USER')}:{os.getenv('LAKE_DB_PASSWORD')}"
        f"@{os.getenv('LAKE_DB_HOST')}:{os.getenv('LAKE_DB_PORT', '5432')}/{os.getenv('LAKE_DB_NAME')}"
    )
    source = sql_database(credentials=lake_url, schema="raw")
    
    # 2. Apply Incremental Loading Hints
    # Configure dlt to only extract new or updated rows based on the 'updated_at' watermark
    for entity in ENTITIES:
        if entity in source.resources:
            source.resources[entity].apply_hints(
                primary_key="id",
                incremental=dlt.sources.incremental("updated_at")
            )
            
    # 3. Configure the Destination Connection (Data Warehouse)
    # Construct the connection string using warehouse-specific environment variables
    warehouse_url = (
        f"postgresql://{os.getenv('WAREHOUSE_USER')}:{os.getenv('WAREHOUSE_PASSWORD')}"
        f"@{os.getenv('WAREHOUSE_HOST')}:{os.getenv('WAREHOUSE_PORT', '5432')}/{os.getenv('WAREHOUSE_DB')}"
    )
    
    # 4. Initialize and Run the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="retailco_lake_to_warehouse",
        destination=dlt.destinations.postgres(warehouse_url),
        dataset_name="raw" # Target schema in the warehouse
    )
    
    # Execute the load using a merge disposition for idempotency 
    load_info = pipeline.run(source, write_disposition="merge")
    
    print("Pipeline finished successfully!")
    print(load_info)
    return load_info

if __name__ == "__main__":
    run_dlt_pipeline()