from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from pendulum import duration

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
        dag_id="gold_table_refresh",
        default_args=default_args,
        start_date=datetime.now() - timedelta(1),
        schedule="@monthly",
        catchup=True,
        max_active_runs=1,
        tags=["gold", "delta", "trino"],
) as dag:
    refresh_daily_trips = BashOperator(
        task_id="refresh_daily_trips",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/gold/agg_daily_trips.py """,
    )

    refresh_hourly_usage = BashOperator(
        task_id="refresh_hourly_usage",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/gold/agg_hourly_usage.py """,
    )

    refresh_station_usage = BashOperator(
        task_id="refresh_station_usage",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/gold/agg_station_usage.py""",
    )

    (
            refresh_daily_trips
            >> refresh_hourly_usage
            >> refresh_station_usage
    )
