from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
        dag_id="bike_share_batch",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False
) as dag:

    spark_batch = SparkSubmitOperator(
        task_id="batch_analytics",
        application="/opt/spark-apps/batch_analytics.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    spark_batch
