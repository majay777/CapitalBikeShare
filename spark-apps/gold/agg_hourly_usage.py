from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("agg_hourly_usage")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("""
MERGE INTO gold.agg_hourly_usage h
USING(
SELECT
d.hour,
COUNT(*) AS total_trips
FROM fact_trips f
JOIN dim_date d
ON f.date_id = d.date_id
GROUP BY d.hour
) g
ON h.hour = g.hour
WHEN MATCHED THEN UPDATE SET  *
WHEN NOT MATCHED THEN INSERT *
""")