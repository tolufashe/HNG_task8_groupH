from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "person_c",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dlt_load_warehouse",
    description="Loads raw data from Lake to Warehouse using dlt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    # Wait for extraction DAG to finish first
    wait_for_extraction = ExternalTaskSensor(
    task_id="wait_for_erp_extract",
    external_dag_id="erp_extract",
    external_task_id="extract_payment_methods",
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
    mode="poke",
    poke_interval=60,
    timeout=3600,
    )

    # Run dlt Python script
    run_dlt = BashOperator(
        task_id="run_dlt_pipeline",
        bash_command="python /opt/airflow/dlt_pipeline/dlt_pipeline.py"
    )

    wait_for_extraction >> run_dlt