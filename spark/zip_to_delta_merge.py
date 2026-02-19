import os
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# -----------------------------
# Spark Session with Delta
# -----------------------------
spark = (
    SparkSession.builder
    .appName("ZIP to Delta with Merge")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

# -----------------------------
# MinIO / S3A config
# -----------------------------
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# -----------------------------
# Paths
# -----------------------------
ZIP_S3_PATH = "s3a://bike-data/raw/station_status.zip"
LOCAL_ZIP = "/tmp/station_status.zip"
EXTRACT_DIR = "/tmp/station_status"
DELTA_PATH = "s3a://bike-data/delta/station_status"

# -----------------------------
# Step 1: Copy ZIP from MinIO
# -----------------------------
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

fs.copyToLocalFile(
    False,
    spark._jvm.org.apache.hadoop.fs.Path(ZIP_S3_PATH),
    spark._jvm.org.apache.hadoop.fs.Path(LOCAL_ZIP)
)

# -----------------------------
# Step 2: Unzip locally
# -----------------------------
os.makedirs(EXTRACT_DIR, exist_ok=True)

with zipfile.ZipFile(LOCAL_ZIP, "r") as zip_ref:
    zip_ref.extractall(EXTRACT_DIR)

# -----------------------------
# Step 3: Read extracted data
# -----------------------------
df = (
    spark.read
    .option("header", True)
    .csv(EXTRACT_DIR)
    .withColumn("date", to_date(col("last_updated")))
)

# Example primary key
PRIMARY_KEY = "station_id"

# -----------------------------
# Step 4: Create Delta table if not exists
# -----------------------------
if not spark._jsparkSession.catalog().tableExists("station_status"):
    (
        df.write
        .format("delta")
        .partitionBy("date")
        .mode("overwrite")
        .save(DELTA_PATH)
    )

    spark.sql(f"""
        CREATE TABLE station_status
        USING DELTA
        LOCATION '{DELTA_PATH}'
    """)

else:
    # -----------------------------
    # Step 5: MERGE / UPSERT
    # -----------------------------
    df.createOrReplaceTempView("updates")

    spark.sql(f"""
        MERGE INTO station_status t
        USING updates s
        ON t.{PRIMARY_KEY} = s.{PRIMARY_KEY}
           AND t.date = s.date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

spark.stop()
