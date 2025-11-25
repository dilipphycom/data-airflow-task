from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import os
import subprocess

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

GCS_BUCKET = os.environ.get("GCS_BUCKET", "your-gcs-bucket")
GCS_PREFIX = "alphavantage/IBM/"
BQ_PROJECT = os.environ.get("PROJECT_ID", "data-task-phycom")
BQ_DATASET = "stage_dataset"
BQ_TABLE = "stocke_market_daily_data"

def run_fetch_script():
    script_path = "/opt/airflow/my_stock_data_task/fetch_alpha_avro.py"
    subprocess.run(["python3", script_path], check=True)

with DAG(
    dag_id="stock_pipeline_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock", "pipeline"],
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=run_fetch_script
    )

    load_avro_to_bigquery = GCSToBigQueryOperator(
        task_id="load_avro_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_PREFIX}*.avro"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="AVRO",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    extract_and_upload >> load_avro_to_bigquery
