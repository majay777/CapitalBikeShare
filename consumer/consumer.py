
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("KafkaToDelta")
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

spark.sparkContext.setLogLevel("WARN")

kafka_servers = "kafka:9092"
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", "events")
    .option("startingOffsets", "earliest")
    .load()
)
schema = StructType([
    StructField("data", StructType([
        StructField("stations", ArrayType(StructType([
            StructField("is_installed", LongType(), True),
            StructField("is_renting", LongType(), True),
            StructField("is_returning", LongType(), True),
            StructField("last_reported", LongType(), True),
            StructField("num_bikes_available", LongType(), True),
            StructField("num_bikes_disabled", LongType(), True),
            StructField("num_docks_available", LongType(), True),
            StructField("num_docks_disabled", LongType(), True),
            StructField("num_ebikes_available", LongType(), True),
            StructField("num_scooters_available", LongType(), True),
            StructField("num_scooters_unavailable", LongType(), True),
            StructField("station_id", StringType(), True),
            StructField("vehicle_types_available", ArrayType(StructType([
                StructField("count", LongType(), True),
                StructField("vehicle_type_id", StringType(), True)
            ]), True), True)
        ]), True), True),
        StructField("last_updated", LongType(), True),
        StructField("ttl", LongType(), True),
        StructField("version", StringType(), True)
    ]))
])



parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*", "last_updated", 'ttl', 'version')
)

query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://delta/checkpoints/train")
    .start("s3a://delta/train_location")
)

query.awaitTermination()

"""
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/opt/spark/ivy --packages io.delta:delta-core_2.12:2.3.0 /opt/spark-apps/consumer.py
"""

"""
KAfka-topic

docker exec -it kafka kafka-topics \
  --create \
  --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

"""
