from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("BikeStationStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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


parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/bike") \
    .start("s3a://bike-data/station_history")
