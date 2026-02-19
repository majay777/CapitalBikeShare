from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
        dag_id="kafka_to_delta_streaming",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    start_streaming = BashOperator(
        task_id="start_spark_streaming",
        bash_command="""
        docker exec spark-master spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-spark_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 \
          /opt/spark-apps/kafka_to_delta.py
        """
    )

    start_streaming
