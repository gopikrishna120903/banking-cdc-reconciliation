"""Airflow DAG: daily banking CDC load with reconciliation gates.

wait_for_control_file -> load_and_reconcile (Databricks) -> notify_success
A reconciliation failure fails the Databricks task, so nothing is published and the
watermark is not advanced; the next run safely retries the same business date.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sensors.filesystem import FileSensor

LANDING = "/mnt/landing/core_banking"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-eng-oncall@example.com"],
}

with DAG(
    dag_id="banking_daily_cdc",
    description="Incremental CDC load of core-banking transactions with control-total reconciliation",
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * 1-5",          # 5 AM on business days, ahead of morning risk reporting
    catchup=False,
    max_active_runs=1,                # loads must run in order for watermarks to stay correct
    default_args=default_args,
    tags=["banking", "cdc", "reconciliation"],
) as dag:
    wait_for_control_file = FileSensor(
        task_id="wait_for_control_file",
        filepath=LANDING + "/control_{{ ds }}.json",
        poke_interval=300,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    load_and_reconcile = DatabricksSubmitRunOperator(
        task_id="load_and_reconcile",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "banking_cdc_{{ ds }}",
            "tasks": [{
                "task_key": "cdc_load",
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "Standard_D4ds_v5",
                    "num_workers": 2,
                },
                "python_wheel_task": {
                    "package_name": "banking_cdc",
                    "entry_point": "run-cdc",
                    "parameters": ["--data-dir", "abfss://banking@<storage>.dfs.core.windows.net",
                                   "--business-date", "{{ ds }}"],
                },
                "libraries": [{"whl": "/Volumes/platform/banking/code/banking_cdc-1.0.0-py3-none-any.whl"}],
            }],
        },
    )

    notify_success = EmptyOperator(task_id="publish_complete")

    wait_for_control_file >> load_and_reconcile >> notify_success
