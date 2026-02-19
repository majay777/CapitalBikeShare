import folium
import pandas as pd
from pyspark.sql import SparkSession
from streamlit_folium import st_folium
from deltalake import DeltaTable
import streamlit as st

st.set_page_config(layout="wide")
st.title("🚲 Bike Share – Historical Analytics")

# spark = (
#     SparkSession.builder
#     .appName("KafkaToDelta")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
#     .getOrCreate()
# )


# @st.cache_data(ttl=60)
# def load_history():
#     return (
#         spark.read
#         .format("delta")
#         .load("s3a://bike-data/station_history")
#         .toPandas()
#     )

import streamlit as st
from deltalake import DeltaTable
import os
print(os.environ)

@st.cache_data(ttl=60)
def load_history():
    dt = DeltaTable(
        "s3://bike-data/station_history",
        storage_options={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT": "http://minio:9000",
            "AWS_S3_URL_STYLE": "path",
            "AWS_ALLOW_HTTP": "true"
        }
    )
    return dt.to_pandas()

df = load_history()


st.subheader("📈 Total Bikes Over Time")

trend = (
    df.groupby("ingest_time")["num_bikes_available"]
    .sum()
    .reset_index()
)

st.line_chart(trend.set_index("ingest_time"))

st.subheader("📊 Station-Level Trend")

station_id = st.selectbox(
    "Station ID",
    df["station_id"].unique()
)

station_df = df[df["station_id"] == station_id]

st.line_chart(
    station_df.set_index("ingest_time")[
        ["num_bikes_available", "num_docks_available"]
    ]
)

st.subheader("📈 Average Station Utilization (%) Over Time")

util_trend = (
    df.groupby("ingest_time")["utilization_pct"]
    .mean()
    .reset_index()
)

st.line_chart(
    util_trend.set_index("ingest_time")
)

st.subheader("🏷️ Station Utilization Trend")

station = st.selectbox(
    "Select Station",
    df["station_id"].unique()
)

station_util = df[df["station_id"] == station]

st.line_chart(
    station_util.set_index("ingest_time")["utilization_pct"]
)

# 🧮 Compute Hourly Usage
df["hour"] = pd.to_datetime(df["ingest_time"]).dt.hour

st.subheader("⏰ Peak Bike Usage Hours")

hourly = (
    df.groupby("hour")["num_bikes_available"]
    .mean()
    .reset_index()
)

st.bar_chart(
    hourly.set_index("hour")
)

st.subheader("🚲 Station-Specific Peak Hours")

station_hourly = (
    df[df["station_id"] == station]
    .groupby("hour")["num_bikes_available"]
    .mean()
    .reset_index()
)

st.bar_chart(
    station_hourly.set_index("hour")
)

df_sorted = df.sort_values(["station_id", "ingest_time"])

df_sorted["is_down"] = df_sorted["num_bikes_available"] == 0

df_sorted["down_duration"] = (
    df_sorted.groupby("station_id")["is_down"]
    .cumsum()
)

st.subheader("🚨 Station Outages")

latest = (
    df_sorted.sort_values("ingest_time")
    .groupby("station_id")
    .tail(1)
)

outages = latest[latest["num_bikes_available"] == 0]

st.dataframe(
    outages[
        ["station_id", "num_bikes_available", "ingest_time"]
    ],
    use_container_width=True
)

st.subheader("📉 Stations With Frequent Outages")

outage_count = (
    df_sorted[df_sorted["num_bikes_available"] == 0]
    .groupby("station_id")
    .size()
    .reset_index(name="outage_events")
    .sort_values("outage_events", ascending=False)
)

st.dataframe(outage_count.head(10))

st.subheader("🗺️ Historical Map Playback")
df["ingest_time"] = df["ingest_time"].dt.to_pydatetime()

# selected_time = st.slider(
#     "Select Timestamp",
#     min_value=1,
#     max_value=9 ,
#     value=1
# )

# snapshot = df[
#     pd.to_datetime(df["ingest_time"]) <= pd.to_datetime(selected_time)
#     ].sort_values("ingest_time").groupby("station_id").tail(1)
#
# snapshot = df[
#     pd.to_datetime(df["ingest_time"])
#     ].sort_values("ingest_time").groupby("station_id").tail(1)
#
#
# m = folium.Map(
#     location=[snapshot["lat"].mean(), snapshot["lon"].mean()],
#     zoom_start=13
# )
#
# for _, row in snapshot.iterrows():
#     color = "red" if row["num_bikes_available"] == 0 else "green"
#
#     folium.CircleMarker(
#         location=[row["lat"], row["lon"]],
#         radius=6,
#         color=color,
#         fill=True,
#         popup=f"""
#         Station: {row['station_id']}<br>
#         Bikes: {row['num_bikes_available']}<br>
#         Utilization: {row['utilization_pct']:.1f}%
#         """
#     ).add_to(m)
#
# st_folium(m, width=1200, height=600)
