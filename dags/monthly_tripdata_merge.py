import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .appName("Monthly Tripdata MERGE")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ---- MinIO config ----
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ---- Paths from Airflow ----
ZIP_PATH = os.environ["ZIP_PATH"]     # s3a://tripdata/raw/year=YYYY/month=MM/....
DELTA_PATH = "s3a://tripdata/delta/trips"

LOCAL_ZIP = "/tmp/trips.zip"
EXTRACT_DIR = "/tmp/trips"

# ---- Copy ZIP locally ----
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
fs.copyToLocalFile(False,
                   spark._jvm.org.apache.hadoop.fs.Path(ZIP_PATH),
                   spark._jvm.org.apache.hadoop.fs.Path(LOCAL_ZIP)
                   )

# ---- Unzip ----
os.makedirs(EXTRACT_DIR, exist_ok=True)
with zipfile.ZipFile(LOCAL_ZIP, "r") as z:
    z.extractall(EXTRACT_DIR)

# ---- Read CSVs ----
df = (
    spark.read
    .option("header", True)
    .csv(EXTRACT_DIR)
    .withColumn("trip_date", to_date(col("started_at")))
)

# ---- Create table if not exists ----
if not spark._jsparkSession.catalog().tableExists("trips"):
    df.write \
        .format("delta") \
        .partitionBy("trip_date") \
        .mode("overwrite") \
        .save(DELTA_PATH)

    spark.sql(f"""
        CREATE TABLE trips
        USING DELTA
        LOCATION '{DELTA_PATH}'
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
