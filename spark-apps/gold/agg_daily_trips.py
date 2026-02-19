from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("agg_daily_trips")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("""
MERGE INTO gold.agg_daily_trips t
USING (
    SELECT
        date_id,
        member_casual,
        COUNT(*) AS total_trips,
        AVG(trip_duration_min) AS avg_duration
    FROM fact_trips
    WHERE date_id = current_date()
    GROUP BY date_id, member_casual
) s
ON t.date_id = s.date_id
AND t.member_casual = s.member_casual
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")