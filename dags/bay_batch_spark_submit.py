from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
with DAG(
        dag_id="bay_dag",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["bike-share", "bay", "analytics"],
)as dag:

    bay_bash_command = BashOperator(
        task_id="bash_command",
        bash_command=" spark-submit  --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.wildfly.openssl:wildfly-openssl:1.0.7.Final --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 --executor-cores 4   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog  /opt/airflow/spark-apps/bay_batch_analytics.py"
    )

    spark_batch = SparkSubmitOperator(
        task_id="bay_bike_analytics",
        application="/opt/spark-apps/bay_batch_analytics.py",
        conn_id="spark_default",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.jars.packages":
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.wildfly.openssl:wildfly-openssl:1.0.7.Final,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.12:2.4.0",

        }

    )

    [bay_bash_command , spark_batch]
