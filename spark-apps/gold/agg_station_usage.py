










from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("agg_daily_trips")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("""
MERGE INTO gold.agg_station_usage st 
USING(
    select start_station_id,
count(*) as trips_started FROM fact_trips GROUP BY start_station_id
) s
on st.start_station_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")