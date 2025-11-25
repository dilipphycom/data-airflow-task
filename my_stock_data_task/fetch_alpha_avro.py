# fetch_alpha_avro.py
import os
import json
import requests
from datetime import datetime
from fastavro import writer, parse_schema
from google.cloud import storage

# CONFIG - change these to match your GCP resources
API_URL = "https://www.alphavantage.co/query"
API_PARAMS = {
    "function": "TIME_SERIES_DAILY",
    "symbol": "IBM",
    #"outputsize": "full",
    "apikey": "JB0SNBHZLVRX1KRS"
}
GCS_BUCKET = os.environ.get("GCS_BUCKET", "my-stock-avro-bucket")  # set via env in DAG or container
GCS_DEST_PREFIX = os.environ.get("GCS_DEST_PREFIX", "alphavantage")
LOCAL_OUT_DIR = os.environ.get("LOCAL_OUT_DIR", "/opt/airflow/my_stock_data_task")

AVRO_SCHEMA = {
  "name": "alphavantage_daily",
  "type": "record",
    "fields": [
        {"name": "symbol", "type": "string"},
        {
            "name": "date",
            "type": {
                "type": "int",
                "logicalType": "date"
            }
        },
        {"name": "open", "type": ["null", "float"]},
        {"name": "high", "type": ["null", "float"]},
        {"name": "low", "type": ["null", "float"]},
        {"name": "close", "type": ["null", "float"]},
        {"name": "volume", "type": ["null", "long"]},
        {"name": "raw_json", "type": ["null", "string"]}
    ]
}

# Convert YYYY-MM-DD → int days since epoch (BigQuery DATE)
def to_avro_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime(1970, 1, 1)
    return (dt - epoch).days


def fetch_api():
    resp = requests.get(API_URL, params=API_PARAMS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def find_time_series_key(raw_json):
    for k in raw_json.keys():
        if "Time Series" in k or "Time_Series" in k:
            return k
    return None

def flatten_and_prepare_records(raw_json):
    ts_key = find_time_series_key(raw_json)
    if not ts_key:
        raise RuntimeError("Time series key not found in response")
    meta = raw_json.get("Meta Data", {})
    ts = raw_json.get(ts_key, {})
    records = []
    for date_str, metrics in ts.items():
        # records.append({
        #     "symbol": meta.get("2. Symbol") or API_PARAMS.get("symbol"),
        #     "date": date_str,
        #     "open": metrics.get("1. open"),
        #     "high": metrics.get("2. high"),
        #     "low": metrics.get("3. low"),
        #     "close": metrics.get("4. close"),
        #     "volume": metrics.get("5. volume"),
        #     "raw_json": json.dumps({"meta": meta, "date": date_str, "metrics": metrics}, ensure_ascii=False)
        # })
        
        records.append({
            "symbol": meta.get("2. Symbol") or API_PARAMS.get("symbol"),
            "date": to_avro_date(date_str),
            "open": float(metrics.get("1. open")) if metrics.get("1. open") else None,
            "high": float(metrics.get("2. high")) if metrics.get("2. high") else None,
            "low": float(metrics.get("3. low")) if metrics.get("3. low") else None,
            "close": float(metrics.get("4. close")) if metrics.get("4. close") else None,
            "volume": int(metrics.get("5. volume")) if metrics.get("5. volume") else None,
            "raw_json": json.dumps({"meta": meta, "date": date_str, "metrics": metrics}, ensure_ascii=False)
        })
    return records

def write_avro(records, out_path):
    parsed = parse_schema(AVRO_SCHEMA)
    with open(out_path, "wb") as out_f:
        writer(out_f, parsed, records)

def upload_to_gcs(local_path, bucket_name, dest_blob):
    client = storage.Client(project="data-task-phycom")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket_name}/{dest_blob}")

def main():
    raw = fetch_api()
    records = flatten_and_prepare_records(raw)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"alphavantage_{API_PARAMS['symbol']}_{today}.avro"
    local_path = os.path.join(LOCAL_OUT_DIR, filename)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    write_avro(records, local_path)
    dest_blob = f"{GCS_DEST_PREFIX}/{API_PARAMS['symbol']}/{filename}"
    upload_to_gcs(local_path, GCS_BUCKET, dest_blob)
    print("Done. Wrote and uploaded", filename)

if __name__ == "__main__":
    main()
