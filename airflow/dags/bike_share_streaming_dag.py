from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
        dag_id="bike_share_streaming_pipeline",
        default_args=DEFAULT_ARGS,
        description="Always-on Kafka → Spark Streaming → Delta",
        start_date=days_ago(1),
        schedule_interval=None,   # MANUAL trigger
        catchup=False,
        tags=["bike-share", "streaming", "bay"],
) as dag:



    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command="""
         spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/opt/spark/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark-apps/stream_station_status.py
        """
    )

    # create_kafka_topic >> run_kafka_producer >> run_spark_streaming
    run_spark_streaming
