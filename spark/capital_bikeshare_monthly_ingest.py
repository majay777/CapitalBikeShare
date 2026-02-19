from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import os
import boto3

# -----------------------
# Config
# -----------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "tripdata"

BASE_URL = "https://s3.amazonaws.com/capitalbikeshare-data"

LOCAL_DIR = "/tmp/bikeshare"
os.makedirs(LOCAL_DIR, exist_ok=True)

# -----------------------
# Task logic
# -----------------------
def download_and_upload(**context):
    execution_date = context["execution_date"]

    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    yyyymm = execution_date.strftime("%Y%m")

    filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
    url = f"{BASE_URL}/{filename}"
    local_path = f"{LOCAL_DIR}/{filename}"

    # ---- Download ZIP ----
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    # ---- Upload to MinIO ----
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3_key = f"raw/year={year}/month={month}/{filename}"

    s3.upload_file(
        local_path,
        BUCKET,
        s3_key
    )

    # Cleanup
    os.remove(local_path)

# -----------------------
# DAG
# -----------------------
with DAG(
        dag_id="capital_bikeshare_monthly_ingest",
        start_date=datetime(2020, 1, 1),
        schedule_interval="@monthly",
        catchup=True,
        max_active_runs=1,
        tags=["bikeshare", "minio", "monthly"],
) as dag:

    ingest_monthly_tripdata = PythonOperator(
        task_id="download_upload_tripdata",
        python_callable=download_and_upload,
        provide_context=True,
    )
