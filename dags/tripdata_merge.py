from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import get_current_context
from pendulum import duration

from airflow.sdk import DAG

with DAG(
        dag_id="bike_tripdata_merge",
        start_date=datetime(2020, 1, 1),
        schedule="@monthly",
        catchup=True,
        tags=["bikeshare", "trip", "analytics"],
) as dag:
    spark_submit = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --packages \
    org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark-apps/tripdata_merge_pandas.py \
          --run_date {{ ds }}
        """,
    )

    spark_submit
