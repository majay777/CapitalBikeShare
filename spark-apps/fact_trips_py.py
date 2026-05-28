from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

# df = spark.read.format("delta").load("s3a://tripdata/delta/trips/")




df3 = spark.read.parquet("s3a://tripdata/parquet/trips/")

print(df3.count(), "count is : ")
print(df3.printSchema())
# fact = (
#     df.withColumn("start_time_id", col("started_at").cast("date")).withColumn("end_time_id", col("ended_at").cast("date"))
#     .withColumn(
#         "trip_duration_min",
#         (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60
#     ).select(
#         "ride_id",
#         "start_time_id",
#         "end_time_id",
#         "rideable_type",
#         "member_casual",
#         col("start_station_id").alias("start_station_id"),
#         col("end_station_id").alias("end_station_id"),
#         "trip_duration_min","start_lat", "start_lng", "end_lat", "end_lng"
#     )
# )
#
# fact.write.format("delta") \
#     .partitionBy("start_time_id") \
#     .mode("overwrite") \
#     .save("s3a://tripdata/delta/fact_trips")

df2 = spark.read.format("delta").load("s3a://tripdata/delta/fact_trips")

print(df2.printSchema())
print(df2.count())

