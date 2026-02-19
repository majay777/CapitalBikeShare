from datetime import timedelta, datetime

from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_batch_analytics():
    spark = (
        SparkSession.builder
        .appName("BikeShareBatch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", (
            " org.wildfly.openssl:wildfly-openssl:1.0.7.Final,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.13:3.1.0", "io.delta:delta-core_2.13:3.1.0"
        # Combined into one string; removed the second .config call
        )).config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    df = spark.read.format("delta") \
        .load("s3a://delta/train_location")

    # df = spark.read.format("delta") \
    #     .load("s3a://bike-data/station_history")

    # -------------------------
    # Utilization %
    # -------------------------
    utilization = df.groupBy("station_id") \
        .agg(avg("utilization_pct").alias("avg_utilization"))

    utilization.write.format("delta") \
        .mode("overwrite") \
        .save("s3a://delta/capital/analytics/utilization")

    # -------------------------
    # Peak Hour Analysis
    # -------------------------
    peak_hours = (
        df.withColumn("hour", hour("ingest_time"))
        .groupBy("hour")
        .agg(avg("num_bikes_available").alias("avg_bikes"))
    )

    peak_hours.write.format("delta") \
        .mode("overwrite") \
        .save("s3a://delta/capital/analytics/peak_hours")

    # -------------------------
    # Outage Detection
    # -------------------------
    outages = df.filter(col("num_bikes_available") == 0) \
        .groupBy("station_id") \
        .agg(count("*").alias("outage_events"))

    outages.write.format("delta") \
        .mode("overwrite") \
        .save("s3a://delta/capital/analytics/outages")

    print("✅ Batch analytics completed")


with DAG(
        dag_id="bike_share_batch_analytics",
        default_args=DEFAULT_ARGS,
        description="Hourly analytics & data quality checks",
        start_date=datetime.now() - timedelta(days=1),
        schedule="0 * * * *",  # every hour
        catchup=False,
        tags=["bike-share", "batch", "analytics"],
) as dag:
    batch_analytics = PythonOperator(
        task_id="run_batch_analytics",
        python_callable=run_batch_analytics
    )

    batch_analytics
