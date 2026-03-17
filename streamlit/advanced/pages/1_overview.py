import streamlit as st
import plotly.express as px
from utils.data_loader import load_data


option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)

df = load_data(option)

st.title("Overview")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Trips", len(df))
col2.metric("Stations", df["start_station_name"].nunique())
col3.metric("Avg Duration", round(df["duration_min"].mean(), 2))
col4.metric("Bike Types", df["rideable_type"].nunique())

st.subheader("Trips Over Time")

daily = df.groupby(df["started_at"].dt.date).size()

fig = px.line(
    x=daily.index,
    y=daily.values,
    labels={"x": "Date", "y": "Trips"}
)

st.plotly_chart(fig, width='stretch')

trips_by_day = (
    df
    .groupby("date")
    .size()
    .reset_index(name="trips")
)

fig = px.line(
    trips_by_day,
    x="date",
    y="trips",
    title="Daily Trips"
)

st.subheader("Member vs Casual Riders")

user_counts = (
    df["member_casual"]
    .value_counts()
    .reset_index()
)

user_counts.columns = ["user_type", "count"]

fig = px.pie(
    user_counts,
    values="count",
    names="user_type"
)

st.plotly_chart(fig, use_container_width=True)

st.subheader("Different Bike Usage")

user_counts = (
    df["rideable_type"]
    .value_counts()
    .reset_index()
)

user_counts.columns = ["bike_type", "count"]

fig = px.pie(
    user_counts,
    values="count",
    names="bike_type"
)

st.plotly_chart(fig, width='stretch')

avg_duration = df.groupby("rideable_type")['duration_min'].mean().reset_index()

fig = px.bar(avg_duration, x="rideable_type", y="duration_min", color="rideable_type",
             title="Average Duration For Bikes")
st.plotly_chart(fig, width='stretch')
