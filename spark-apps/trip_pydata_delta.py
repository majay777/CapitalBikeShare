import argparse
import sys
import traceback

from pyspark.sql import SparkSession

BUCKET_NAME = "tripdata"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
# #

parser = argparse.ArgumentParser(description="Trip PyData Delta Job")

parser.add_argument(
    "--run_date",
    required=True,
    help="Execution date in YYYY-MM-DD format"
)

args = parser.parse_args()
run_date = args.run_date

year = run_date.split("-")[0]
month = run_date.split("-")[1]
month = f"{int(month):02d}"
yyyymm = year + month

print(type(year))
print(type(month))
filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
s3_key = f"parquet/trips/year={year}/month={month}/*"

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()

)

# ---- MinIO config ----
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

try:
    # your existing Spark code
    # spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    import pyspark.sql.functions as F
    df = spark.read.format("parquet").load(f"s3a://tripdata/{s3_key}/")
    df = df.withColumn("year", F.year(F.col("started_at"))).withColumn("month", F.month(F.col("started_at")))
    if not spark._jsparkSession.catalog().tableExists("trips"):
        df.write \
            .format("delta") \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .save("s3a://tripdata/delta/trips/")

        spark.sql(f"""
            CREATE TABLE trips
            USING DELTA
            LOCATION 's3a://tripdata/delta/trips/'
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

except Exception as e:
    print("=== PYTHON EXCEPTION ===", file=sys.stderr)
    traceback.print_exc()
    raise
