# Stock ELT pipeline (AlphaVantage → Avro → GCS → BigQuery → Dataform)

## Setup (Windows, Docker Compose)
1. Put service account file at:
   `D:\Airflow\secrets\data-task-phycom.json`

2. Edit `.env` to set `AIRFLOW_PROJ_DIR=D:/Airflow`.

3. Update `dags/stock_pipeline_dag.py`:
   - set `PROJECT_ID` to your GCP project id
   - set `GCS_BUCKET` to your bucket name

4. Build custom image (installs GCP libs permanently):
   ```powershell
   cd D:\Airflow\my_stock_data_task
   docker compose down --volumes --remove-orphans
   docker compose build
   docker compose up -d
