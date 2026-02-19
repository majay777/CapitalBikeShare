import argparse
import io
import shutil
import tempfile
import zipfile

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
BUCKET_NAME = "tripdata"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
# #

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

parser = argparse.ArgumentParser()
parser.add_argument("--run_date", required=True)
args = parser.parse_args()

run_date = args.run_date  # YYYY-MM-DD

# run_date = '2024-12-01'
year = run_date.split("-")[0]
month = run_date.split("-")[1]
yyyymm = year + month

filename = f"{yyyymm}-capitalbikeshare-tripdata.zip"
s3_key = f"raw/year={year}/month={month}/{filename}"

# Initialize MinIO client
client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# Get the ZIP object
response = client.get_object("tripdata", s3_key)
zip_data = io.BytesIO(response.read())

with zipfile.ZipFile(zip_data) as z:
    file_name = z.namelist()[0]

    with z.open(file_name) as f:
        # create a temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            shutil.copyfileobj(f, tmp)
            tmp_path = tmp.name

# read with Spark
df = spark.read.csv(tmp_path, header=True, inferSchema=True)
df = df.withColumn("year", year(col("started_at"))).withColumn("month", month(col("started_at")))
df.write.partitionBy("year", "month").format("delta").mode("overwrite").save("s3a://tripdata/parquet/trips/")
#
# with zipfile.ZipFile(zip_data) as z:
#     # Assuming the ZIP contains a CSV file
#     file_name = z.namelist()[0]
#     with z.open(file_name) as f:
#         # Use Pandas for the initial read (best for smaller files)
#         # pdf = pd.read_csv(f)
#         pdf = spark.read.csv(f, header=True, inferSchema=True)
#         # print(pdf)
#         df = pdf.withColumn("year", year("started_at")) \
#             .withColumn("month", month("started_at"))
#         df.write.partitionBy("year", "month").format("delta").mode("overwrite").save("s3a://tripdata/parquet/trips/")
#         # df.write \
#         #     .partitionBy("year", "month") \
#         #         .mode("append") \
#         #         .delta("s3a://tripdata/parquet/trips/")
#         # pdf.to_parquet(partition_cols=['started_at'], path="s3a://tripdata/parquet/trips/")
