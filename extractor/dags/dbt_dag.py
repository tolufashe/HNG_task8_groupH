from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "person_d",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="dbt_transform",
    description="Runs dbt snapshots, staging, marts and tests after dlt load",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False, # Sensor date-alignment: dbt_transform runs daily alongside dlt_load_warehouse.
                   # To backfill, trigger erp_extract, dlt_load_warehouse, and dbt_transform
                   # together for the same date range using: airflow dags trigger --exec-date <date>
    max_active_runs=1,
    tags=["checkpoint-5", "transformation"],
) as dag:

    wait_for_load = ExternalTaskSensor(
        task_id="wait_for_dlt_load",
        external_dag_id="dlt_load_warehouse",
        external_task_id="run_dlt_pipeline",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/retailco_dbt && dbt snapshot --profiles-dir /opt/airflow/retailco_dbt",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="cd /opt/airflow/retailco_dbt && dbt run --select staging --profiles-dir /opt/airflow/retailco_dbt",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="cd /opt/airflow/retailco_dbt && dbt run --select marts --profiles-dir /opt/airflow/retailco_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/retailco_dbt && dbt test --profiles-dir /opt/airflow/retailco_dbt",
    )

    wait_for_load >> dbt_snapshot >> dbt_staging >> dbt_marts >> dbt_test