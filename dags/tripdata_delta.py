from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import get_current_context
from pendulum import duration

from airflow.sdk import DAG

with DAG(
        dag_id="tripdata_delta",
        start_date=datetime(2024, 1, 1),
        schedule="@monthly",
        catchup=True,
        tags=["bikeshare", "trip", "analytics"],
) as dag:
    spark_submit = BashOperator(
        task_id="parquet_to_delta",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/trip_pydata_delta.py --run_date {{ ds }} """,
    )

    spark_submit