# # stock_pipeline_dag.py
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# from datetime import datetime, timedelta
# import os
# from my_stock_data_task.sql_runner import run_sqlx

# # -----------------------
# # CONFIG - replace with your values
# # -----------------------
# PROJECT_ID = os.environ.get("PROJECT_ID", "data-task-phycom")
# GCS_BUCKET = os.environ.get("GCS_BUCKET", "your-gcs-bucket")
# STAGE_DATASET = "stage"
# FINAL_DATASET = "final"
# STAGE_TABLE = "stocke_market_daily_data"
# FINAL_TABLE = "stocke_market_daily_data"
# # GCS path template (DAG uses yesterday's file by default or ds)
# GCS_SOURCE_TEMPLATE = "alphavantage/IBM/alphavantage_IBM_{{ ds }}.avro"

# SQLX_PATH = "/opt/airflow/my_stock_data_task/definitions/stocke_market_daily_data.sqlx"

# default_args = {
#     "owner": "data-engineer",
#     "depends_on_past": False,
#     "retries": 3,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="stock_pipeline_dag",
#     default_args=default_args,
#     schedule_interval="@daily",
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     tags=["stock", "elt"],
# ) as dag:

#     load_avro_to_bq = GCSToBigQueryOperator(
#         task_id="load_avro_to_bq",
#         bucket=GCS_BUCKET,
#         source_objects=[GCS_SOURCE_TEMPLATE],
#         destination_project_dataset_table=f"{PROJECT_ID}.{STAGE_DATASET}.{STAGE_TABLE}",
#         source_format="AVRO",
#         write_disposition="WRITE_TRUNCATE",
#         autodetect=True,
#         # allow jagged? If you have nested/raw JSON inside the Avro use autodetect=False and provide schema
#     )

#     transform_to_final = PythonOperator(
#         task_id="transform_to_final",
#         python_callable=run_sqlx,
#         op_kwargs={
#             "sqlx_file_path": SQLX_PATH,
#             "project_id": PROJECT_ID,
#             "dataset_id": FINAL_DATASET,
#             "table_name": FINAL_TABLE,
#         },
#     )

#     load_avro_to_bq >> transform_to_final

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
