from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def run_quickshop_etl(date_str: str, dry_run: bool = False, **kwargs):
    logging.info("Task run_quickshop_etl started for date %s (dry_run=%s)", date_str, dry_run)
    # runtime import to avoid parse-time dependency issues
    from quickshop_etl.cli import run_for_date
    data_dir = "/usr/local/airflow/project/data"
    output_dir = "/usr/local/airflow/project/output"
    summary = run_for_date(date_str, data_dir=data_dir, output_dir=output_dir, dry_run=dry_run)
    logging.info("ETL summary: %s", summary)
    return summary

with DAG(
    dag_id="quickshop_etl_pipeline",
    schedule=None,                 # no automatic schedule; trigger manually
    start_date=datetime(2025, 10, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["quickshop", "etl"],
) as dag:

    run_etl_task = PythonOperator(
        task_id="run_quickshop_etl",
        python_callable=run_quickshop_etl,
        op_kwargs={
            "date_str": "{{ ds_nodash }}",
            "dry_run": False,
        },
    )

    run_etl_task
