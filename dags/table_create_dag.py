from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from pendulum import duration

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
        dag_id="table_generate",
        default_args=default_args,
        start_date=datetime.now() - timedelta(1),
        schedule="@once",
        catchup=True,
        max_active_runs=1,
        tags=["delta","table"],
) as dag:
    generate_tables = BashOperator(
        task_id="generate_tables",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/generate_schema.py """,
    )

    gold_table_generate = BashOperator(
        task_id="gold_table_generate",
        bash_command="""
        spark-submit --master spark://spark-master:7077  --executor-cores 3  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/spark-apps/gold/add_generate_tables.py """,
    )


    generate_tables >> gold_table_generate
