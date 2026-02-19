import io
import zipfile
from datetime import timedelta, datetime

import pandas as pd
import s3fs
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from minio import Minio
from pendulum import duration


@dag(dag_id="tripdata_merge_pandas", schedule='@monthly', start_date=datetime(2020, 1, 1),
     description='Data Extract from Coin-cap API', tags=['bikeshare', 'zip'], catchup=False, default_args={
        'owner': 'ajay',
        'retries': 2,
        'retry_delay': duration(seconds=10)}, max_active_runs=1, dagrun_timeout=timedelta(minutes=10))
def tripdata_merge_pandas():
    @task()
    def tripdata():
        context = get_current_context()

        start = context["data_interval_start"]

        year = start.strftime("%Y")
        month = start.strftime("%m")
        yyyymm = start.strftime("%Y%m")

        filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
        s3_key = f"raw/year={year}/month={month}/{filename}"
        print(s3_key)
        # Initialize MinIO client
        client = Minio("minio:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

        schema = {
            "ride_id": "string",
            "rideable_type": "string",
            "started_at": "string",
            "ended_at": "string",
            "start_station_name": "string",
            "start_station_id": "string",
            "end_station_name": "string",
            "end_station_id": "string",
            "start_lat": "string",
            "start_lng": "string",
            "end_lat": "string",
            "end_lng": "string",
            "member_casual": "string"
        }

        # Create an S3 filesystem object
        fs = s3fs.S3FileSystem()

        # Get the ZIP object
        response = client.get_object("tripdata", s3_key)
        zip_data = io.BytesIO(response.read())

        with zipfile.ZipFile(zip_data) as z:
            # Assuming the ZIP contains a CSV file
            file_name = z.namelist()[0]
            with z.open(file_name) as f:
                # Use Pandas for the initial read (best for smaller files)
                pdf = pd.read_csv(f)
                # Parse timestamps safely
                if "started_at"  in pdf.columns:
                    pdf["started_at"] = pd.to_datetime(pdf["started_at"], errors="coerce")

                if "ended_at" in pdf.columns:
                    pdf["ended_at"] = pd.to_datetime(pdf["ended_at"], errors="coerce")

                # Partition columns (SAFE for MinIO)
                pdf["year"] = pdf["started_at"].dt.year.fillna(int(year)).astype(int).astype(str)
                pdf["month"] = pdf["started_at"].dt.month.fillna(int(month)).astype(int).astype(str).str.zfill(2)
                string_cols = [
                    "start_station_id",
                    "end_station_id",
                    "start_station_name",
                    "end_station_name",
                ]

                for col in string_cols:
                    if col in pdf.columns:
                        pdf[col] = pdf[col].astype("string")

        pdf.to_parquet(
                    path="s3://tripdata/parquet/trips/",
                    engine="pyarrow",
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                    partition_cols=["year", "month"],
                    storage_options={
                        "key": "minioadmin",
                        "secret": "minioadmin",
                        "client_kwargs": {
                            "endpoint_url": "http://minio:9000"
                }
                }
            )



    tripdata()


tripdata_merge_pandas()
