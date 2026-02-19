from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count

spark = (
    SparkSession.builder
    .appName("BikeShareBatch")
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
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

# df = spark.read.format("delta") \
#     .load("s3a://delta/train_location")

df = spark.read.format("delta") \
    .load("s3a://bike-data/station_history")

# -------------------------
# Utilization %
# -------------------------
utilization = df.groupBy("station_id") \
    .agg(avg("utilization_pct").alias("avg_utilization"))

utilization.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://delta/capital/analytics/utilization")

# -------------------------
# Peak Hour Analysis
# -------------------------
peak_hours = (
    df.withColumn("hour", hour("ingest_time"))
    .groupBy("hour")
    .agg(avg("num_bikes_available").alias("avg_bikes"))
)

peak_hours.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://delta/capital/analytics/peak_hours")

# -------------------------
# Outage Detection
# -------------------------
outages = df.filter(col("num_bikes_available") == 0) \
    .groupBy("station_id") \
    .agg(count("*").alias("outage_events"))

outages.write.format("delta") \
    .mode("overwrite") \
    .save("s3a://delta/capital/analytics/outages")

print("✅ Batch analytics completed")