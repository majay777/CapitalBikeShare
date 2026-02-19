from airflow import DAG

from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta, datetime
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
        dag_id="bike_share_producer_bay",
        default_args=DEFAULT_ARGS,
        description="Always-on Kafka → Spark Streaming → Delta",
        start_date=datetime.now() - timedelta(days=1),
        schedule=None,   # MANUAL trigger
        catchup=False,
        tags=["bike-share", "producer"],
) as dag:
    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command="""
        python
        /opt/spark/bike_producer.py
        """
    )

    # create_kafka_topic >> run_kafka_producer >> run_spark_streaming
    run_spark_streaming
