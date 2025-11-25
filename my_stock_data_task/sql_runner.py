# sql_runner.py
from google.cloud import bigquery
import os

def run_sqlx(sqlx_file_path: str, project_id: str, dataset_id: str, table_name: str):
    """
    Read SQLX file, replace {{ project_id }} placeholder and execute query
    storing result to destination table (WRITE_TRUNCATE).
    """
    client = bigquery.Client(project=project_id)

    with open(sqlx_file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    sql = sql.replace("{{ project_id }}", project_id)

    destination = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.QueryJobConfig(destination=destination, write_disposition="WRITE_TRUNCATE")

    print("Executing SQL ->", destination)
    query_job = client.query(sql, job_config=job_config)
    query_job.result()  # wait
    print("Completed:", destination)
