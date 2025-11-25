from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="dataform_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,         # instead of schedule_interval
    catchup=False,
    default_args=default_args
):

    # Path inside Docker container
    DATAFORM_PROJECT_DIR = "/opt/dataform_project"
    GOOGLE_CRED_PATH = "/opt/airflow/secrets/data-task-phycom.json"

    # Ensure the directory exists
    ensure_dir = BashOperator(
        task_id="check_dataform_dir",
        bash_command=f"ls -la {DATAFORM_PROJECT_DIR}"
    )

    # Install dependencies (Silent if already installed)
    install_deps = BashOperator(
        task_id="install_dataform_deps",
        bash_command=f"""
        cd {DATAFORM_PROJECT_DIR} && \
        echo 'Installing NPM deps...' && \
        npm install || true
        """
    )

    # Run Dataform
    run_dataform = BashOperator(
        task_id="run_dataform",
        bash_command=f"""
        export GOOGLE_APPLICATION_CREDENTIALS={GOOGLE_CRED_PATH}
        cd {DATAFORM_PROJECT_DIR}
        echo 'Running Dataform...'
        dataform run --json &> /tmp/dataform_output.log
        cat /tmp/dataform_output.log
        """,
    )

    ensure_dir >> install_deps >> run_dataform




# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5)
# }

# with DAG(
#     dag_id="dataform_dag",
#     start_date=datetime(2025, 11, 1),
#     schedule=None,
#     catchup=False,
# ) as dag:

#     run_dataform = BashOperator(
#         task_id="run_dataform",
#         bash_command="""
#     gcloud dataform versions run \
#         --project=data-task-phycom \
#         --region=us-central1 \
#         --repository=dataform_repo""",
#     )

#     run_dataform
