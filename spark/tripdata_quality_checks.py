from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

df = spark.table("trips")

errors = []

if df.count() == 0:
    errors.append("Trip table is empty")

if df.filter(col("started_at").isNull()).count() > 0:
    errors.append("Null started_at values found")

if df.filter(col("start_station_id").isNull()).count() > 0:
    errors.append("Null start_station_id found")

if df.filter(col("ended_at") <= col("started_at")).count() > 0:
    errors.append("Invalid trip durations")

if errors:
    raise Exception("DATA QUALITY FAILED: " + " | ".join(errors))

spark.stop()
