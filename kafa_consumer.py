from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from typing import List
spark = SparkSession.builder.appName('Kafka').getOrCreate()


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

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()


# query = (
#     parsed.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "/checkpoints/train")
#     .start("/delta/train_location")
# )
#
# query.awaitTermination()

"""
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic bike-occupancy \
  --from-beginning

"""