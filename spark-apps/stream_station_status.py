from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.sql.types import *

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

schema = StructType([
    StructField("station_id", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_docks_available", IntegerType()),
    StructField("event_time", StringType())
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bike_station_status")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingest_time", current_timestamp())
)

parsed = parsed.withColumn(
    "utilization_pct",
    expr("""
      CASE 
        WHEN (num_bikes_available + num_docks_available) > 0
        THEN (num_bikes_available * 100.0) /
             (num_bikes_available + num_docks_available)
        ELSE 0
      END
    """)
)

query = (
    parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/checkpoints/bike")
    .start("s3a://bike-data/station_history")
)

query.awaitTermination()
