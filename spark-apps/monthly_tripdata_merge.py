# # import os
# # import re
# # import boto3
# # import zipfile
# # from botocore.client import Config
# # # LOCAL_ZIP = "/tmp/"
# # # EXTRACT_DIR = "/tmp/trips"
# # # os.makedirs(LOCAL_ZIP, exist_ok=True)
# #
# # BUCKET_NAME = "tripdata"
# # MINIO_ENDPOINT = "http://minio:9000"
# # MINIO_ACCESS_KEY = "minioadmin"
# # MINIO_SECRET_KEY = "minioadmin"
# #
# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, to_date
# # import argparse
# #
# # # parser = argparse.ArgumentParser()
# # # parser.add_argument("--run_date", required=True)
# # # args = parser.parse_args()
# #
# # # run_date = args.run_date  # YYYY-MM-DD
# #
# # run_date = '2024-12-01'
# # year = run_date.split("-")[0]
# # month = run_date.split("-")[1]
# # yyyymm = year + month
# #
# # BUCKET_NAME = "tripdata"
# # filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
# # s3_key = f"raw/year={year}/month={month}/{filename}"
# #
# # s3_client = boto3.client(
# #     "s3",
# #     endpoint_url=MINIO_ENDPOINT,
# #     aws_access_key_id=MINIO_ACCESS_KEY,
# #     aws_secret_access_key=MINIO_SECRET_KEY,
# #     config=Config(signature_version="s3v4"),
# # )
# # local_dir = "/tmp/"
# # os.makedirs(local_dir, exist_ok=True)
# #
# # zip_path = f"{local_dir}/tripdata.zip"
# #
# # s3_client.download_file(BUCKET_NAME, s3_key, zip_path)
# #
# # spark = (
# #     SparkSession.builder
# #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# #     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
# #     .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
# #     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
# #     .config("spark.hadoop.fs.s3a.path.style.access", "true")
# #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
# #     .getOrCreate()
# #
# # )
# #
# # # ---- MinIO config ----
# # hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
# # hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
# # hadoop_conf.set("fs.s3a.access.key", "minioadmin")
# # hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
# # hadoop_conf.set("fs.s3a.path.style.access", "true")
# # hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# #
# # # ---- Read CSVs ----
# #
# # with zipfile.ZipFile(zip_path, "r") as z:
# #     z.extractall(local_dir)
# #
# # df = spark.read.option("header", "true").csv(f"{local_dir}/*.csv")
# # # df = spark.read.option("header", "true").csv(s3_key)
# #
# #
# # def clean_columns(df):
# #     global clean
# #     for c in df.columns:
# #         clean = re.sub(r'[ ,;{}()\n\t=]', '_', c).lower()
# #
# #     df = df.withColumnRenamed(c, clean)
# #     return df
# #
# #
# # df = clean_columns(df)
# # df = df.withColumn("trip_date", to_date(col("started_at")))
# #
# # # ---- Create table if not exists ----
# # if not spark._jsparkSession.catalog().tableExists("trips"):
# #     df.write \
# #         .format("delta") \
# #         .partitionBy("trip_date") \
# #         .mode("overwrite") \
# #         .save("s3a://tripdata/delta/trips")
# #
# #     spark.sql(f"""
# #                 CREATE TABLE trips
# #                 USING DELTA
# #                 LOCATION s3a://tripdata/delta/trips
# #             """)
# # else:
# #     df.createOrReplaceTempView("updates")
# #
# #     spark.sql("""
# #                 MERGE INTO trips t
# #                 USING updates s
# #                 ON t.started_at = s.started_at
# #                    AND t.start_station_id = s.start_station_id
# #                    AND t.end_station_id = s.end_station_id
# #                    AND t.rideable_type = s.rideable_type
# #                 WHEN MATCHED THEN UPDATE SET *
# #                 WHEN NOT MATCHED THEN INSERT *
# #             """)
# #
# # spark.stop()
#
#
# import os
# import re
# import zipfile
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date
#
# # ------------------------
# # Parameters
# # ------------------------
# run_date = "2024-12-01"
# year, month, _ = run_date.split("-")
# yyyymm = f"{year}{month}"
#
# BUCKET_NAME = "tripdata"
# ZIP_NAME = f"{yyyymm}-capitalbikeshare-tripdata.zip"
# ZIP_S3_PATH = f"s3a://{BUCKET_NAME}/raw/year={year}/month={month}/{ZIP_NAME}"
#
# LOCAL_EXTRACT_DIR = "/tmp/trips"
#
# # ------------------------
# # Spark Session
# # ------------------------
# spark = (
#     SparkSession.builder
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .getOrCreate()
# )
#
# # ------------------------
# # Download ZIP to Spark driver
# # ------------------------
# from pyspark import SparkFiles
#
# spark.sparkContext.addFile(ZIP_S3_PATH)
# local_zip = SparkFiles.get(ZIP_NAME)
#
# # ------------------------
# # Extract ZIP
# # ------------------------
# os.makedirs(LOCAL_EXTRACT_DIR, exist_ok=True)
#
# with zipfile.ZipFile(local_zip, "r") as z:
#     z.extractall(LOCAL_EXTRACT_DIR)
#
# # ------------------------
# # Read CSV
# # ------------------------
# df = (
#     spark.read
#     .option("header", "true")
#     .csv(f"file://{LOCAL_EXTRACT_DIR}/*.csv")
# )
#
# # ------------------------
# # Clean column names
# # ------------------------
# for c in df.columns:
#     clean = re.sub(r"[ ,;{}()\n\t=]", "_", c).lower()
#     df = df.withColumnRenamed(c, clean)
#
# df = df.withColumn("trip_date", to_date(col("started_at")))
#
# # ------------------------
# # Delta write
# # ------------------------
# delta_path = "s3a://tripdata/delta/trips"
#
# if not spark.catalog.tableExists("trips"):
#     (
#         df.write
#         .format("delta")
#         .partitionBy("trip_date")
#         .mode("overwrite")
#         .save(delta_path)
#     )
#
#     spark.sql(f"""
#         CREATE TABLE trips
#         USING DELTA
#         LOCATION '{delta_path}'
#     """)
# else:
#     df.createOrReplaceTempView("updates")
#
#     spark.sql("""
#         MERGE INTO trips t
#         USING updates s
#         ON  t.started_at = s.started_at
#         AND t.start_station_id = s.start_station_id
#         AND t.end_station_id = s.end_station_id
#         AND t.rideable_type = s.rideable_type
#         WHEN MATCHED THEN UPDATE SET *
#         WHEN NOT MATCHED THEN INSERT *
#     """)
#
# spark.stop()


import os
import zipfile
from pyspark.sql import SparkSession

# -------------------------------
# Config
# -------------------------------
ZIP_S3_PATH = "s3a://tripdata/raw/year=2024/month=12/202412-capitalbikeshare-tripdata.zip"
EXTRACT_DIR = "/tmp/tripdata_extracted"

# Make sure extraction directory exists
os.makedirs(EXTRACT_DIR, exist_ok=True)

# -------------------------------
# Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("MonthlyTripDataMerge") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# -------------------------------
# Fetch zip file from S3/MinIO
# -------------------------------
local_zip_path = "/tmp/temp_tripdata.zip"
spark.sparkContext.addFile(ZIP_S3_PATH)  # Downloads S3 file to driver temp

# Spark saves S3 file into a temporary location
from pyspark import SparkFiles
zip_path = SparkFiles.get(os.path.basename(ZIP_S3_PATH))

# -------------------------------
# Extract CSV from zip
# -------------------------------
with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(EXTRACT_DIR)

# Assume zip contains a single CSV
csv_files = [os.path.join(EXTRACT_DIR, f) for f in os.listdir(EXTRACT_DIR) if f.endswith(".csv")]
if not csv_files:
    raise FileNotFoundError(f"No CSV found in {zip_path}")

csv_path = csv_files[0]

# -------------------------------
# Read CSV with Spark
# -------------------------------
df = spark.read.option("header", True).csv(csv_path)
df.show(5)

# -------------------------------
# Example: Write as Delta Table
# -------------------------------
OUTPUT_PATH = "s3a://tripdata/delta/merged_monthly_tripdata"
df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)

print("✅ Trip data processed and saved to Delta successfully!")

