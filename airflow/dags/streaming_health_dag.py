import subprocess

def check_streaming_job():
    result = subprocess.run(
        ["spark-submit", "--status"],
        capture_output=True,
        text=True
    )

    if "RUNNING" not in result.stdout:
        raise Exception("Spark Streaming Job is NOT running")


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
        dag_id="streaming_health_check_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval="*/5 * * * *",
        catchup=False,
) as dag:

    health_check = PythonOperator(
        task_id="check_stream_health",
        python_callable=check_streaming_job,
        retries=2
    )

