from datetime import timedelta, datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import get_current_context
from pendulum import duration
import re


@dag(dag_id="tripdata_merge", schedule='@monthly', start_date=datetime(2020, 1, 1),
     description='Data Extract from Coin-cap API', tags=['bikeshare', 'zip'], catchup=False, default_args={
        'owner': 'ajay',
        'retries': 2,
        'retry_delay': duration(seconds=10)}, max_active_runs=1, dagrun_timeout=timedelta(minutes=10))
def tripdata_merge():
    @task()
    def tripdata():
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_date

        context = get_current_context()

        start = context["data_interval_start"]

        year = start.strftime("%Y")
        month = start.strftime("%m")
        yyyymm = start.strftime("%Y%m")
        # year = '2025'
        # month = '01'
        # yyyymm = '202501'

        filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
        s3_key = f"s3a://tripdata/raw/year={year}/month={month}/{filename}"


        spark = (
            SparkSession.builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
            .getOrCreate()

        )

        # ---- MinIO config ----
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "minioadmin")
        hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


        # ---- Read CSVs ----
        df= spark.read.option("header", "true").csv(s3_key)

        def clean_columns(df):
            global clean
            for c in df.columns:
                clean = re.sub(r'[ ,;{}()\n\t=]', '_', c).lower()

            df = df.withColumnRenamed(c, clean)
            return df

        df = clean_columns(df)
        df = df.withColumn("trip_date", to_date(col("started_at")))

        # ---- Create table if not exists ----
        if not spark._jsparkSession.catalog().tableExists("trips"):
            df.write \
                .format("delta") \
                .partitionBy("trip_date") \
                .mode("overwrite") \
                .save("s3a://tripdata/delta/trips")

            spark.sql(f"""
                CREATE TABLE trips
                USING DELTA
                LOCATION s3a://tripdata/delta/trips
            """)
        else:
            df.createOrReplaceTempView("updates")

            spark.sql("""
                MERGE INTO trips t
                USING updates s
                ON t.started_at = s.started_at
                   AND t.start_station_id = s.start_station_id
                   AND t.end_station_id = s.end_station_id
                   AND t.rideable_type = s.rideable_type
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        spark.stop()

    tripdata()


tripdata_merge()
