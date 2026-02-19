from pyspark.sql import SparkSession

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

# agg_daily_trips

spark.sql("""CREATE OR REPLACE TABLE delta.`s3a://tripdata/gold/agg_daily_trips` USING DELTA
AS
SELECT
start_time_id as date_id,
member_casual,
COUNT(*)        AS total_trips,
AVG(trip_duration_min) AS avg_duration
FROM delta.`s3a://tripdata/delta/fact_trips`
GROUP BY date_id, member_casual;
""")

# agg_station_use

spark.sql("""CREATE OR REPLACE TABLE delta.`s3a://tripdata/gold/agg_station_usage` USING DELTA
AS
SELECT
start_station_id,
COUNT(*) AS trips_started
FROM delta.`s3a://tripdata/delta/fact_trips`
GROUP BY start_station_id;
""")

# agg_hourly_use
spark.sql("""
CREATE OR REPLACE TABLE delta.`s3a://tripdata/gold/agg_hourly_usage` USING DELTA
AS
SELECT
d.hour,
COUNT(*) AS total_trips
FROM delta.`s3a://tripdata/delta/fact_trips` f
JOIN delta.`s3a://tripdata/delta/dim_date` d
ON f.start_time_id = d.date_id
GROUP BY d.hour;
          """)