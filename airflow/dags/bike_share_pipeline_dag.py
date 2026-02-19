from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
        dag_id="bike_share_kafka_spark_delta_pipeline",
        default_args=DEFAULT_ARGS,
        description="Bike Share Kafka → Spark → Delta Lake Pipeline",
        start_date=days_ago(1),
        schedule_interval="*/30 * * * *",  # every 30 minutes
        catchup=False,
        tags=["bike-share", "kafka", "spark", "delta"],
) as dag:

    # -----------------------------
    # 1. Create Kafka Topic
    # -----------------------------
    create_kafka_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command="""
        kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic bike_station_status \
        --partitions 3 \
        --replication-factor 1
        """,
    )

    # -----------------------------
    # 2. Run Kafka Producer
    # -----------------------------
    run_kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="""
        python /opt/kafka/producer.py
        """,
    )

    # -----------------------------
    # 3. Run Spark Streaming Job
    # -----------------------------
    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command="""
        spark-submit \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark-apps/stream_station_status.py
        """,
    )

    # -----------------------------
    # 4. Delta Lake Data Quality Check
    # -----------------------------
    def delta_quality_check():
        spark = SparkSession.builder.getOrCreate()

        df = spark.read.format("delta") \
            .load("s3a://bike-data/station_history")

        if df.count() == 0:
            raise ValueError("❌ Delta table is empty")

        invalid = df.filter(
            (df.num_bikes_available < 0) |
            (df.num_docks_available < 0)
        ).count()

        if invalid > 0:
            raise ValueError("❌ Data quality check failed")

        print("✅ Delta data quality check passed")

    delta_quality_check_task = PythonOperator(
        task_id="delta_quality_check",
        python_callable=delta_quality_check,
    )

    # -----------------------------
    # 5. Success Notification
    # -----------------------------
    success_notification = BashOperator(
        task_id="success_notification",
        bash_command='echo "🚲 Bike Share Pipeline completed successfully!"'
    )

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    (
            run_spark_streaming
            >> delta_quality_check_task
            >> success_notification
    )
