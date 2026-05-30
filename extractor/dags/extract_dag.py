from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import sys

# Import Extractor 
sys.path.insert(0, "/opt/airflow/extractor")

from erp_extractor import extract_entity

# Default dag config
default_args = {
    "owner": "person_b",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(minutes=5),

    "retry_exponential_backoff": True,

    "max_retry_delay": timedelta(minutes=30),
}

# Entity List
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

# Dag Definition
with DAG(
    dag_id="erp_extract",

    description="Daily ERP API extraction into raw Postgres lake",

    default_args=default_args,

    start_date=datetime(2024, 1, 1),

    schedule_interval="@daily",

    catchup=True,

    max_active_runs=1,

    tags=["checkpoint-2", "extraction"],

) as dag:

    tasks = {}

    # Create Task
    for entity in ENTITIES:

        tasks[entity] = PythonOperator(

            task_id=f"extract_{entity}",

            python_callable=extract_entity,

            op_args=[entity],

            execution_timeout=timedelta(hours=1),
        )

    # Dependencies

    # order_items depends on orders existing first

    tasks["orders"] >> tasks["order_items"]
