from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import zipfile
import boto3

MINIO_ENDPOINT = "http://minio:9000"   # use localhost:9000 if not docker
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "airflow-artifacts"

SOURCE_DIR = "/opt/airflow/logs"
ZIP_PATH = "/tmp/airflow_logs.zip"
OBJECT_NAME = "logs/airflow_logs.zip"


def zip_and_upload():
    # --- Zip directory ---
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(SOURCE_DIR):
            for file in files:
                full_path = os.path.join(root, file)
                zipf.write(
                    full_path,
                    arcname=os.path.relpath(full_path, SOURCE_DIR)
                )

    # --- Upload to MinIO ---
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.upload_file(ZIP_PATH, BUCKET_NAME, OBJECT_NAME)


with DAG(
        dag_id="zip_and_upload_to_minio",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
) as dag:

    zip_upload = PythonOperator(
        task_id="zip_and_upload",
        python_callable=zip_and_upload
    )
