import streamlit as st
import plotly.express as px
from utils.data_loader import load_data

option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)

df = load_data(option)

st.title("Trip Patterns")

hourly = df.groupby("hour").size()

fig = px.bar(
    x=hourly.index,
    y=hourly.values,
    labels={"x": "Hour", "y": "Trips"}
)

st.plotly_chart(fig, width='stretch')

weekday = df.groupby("day").size()

fig2 = px.bar(
    x=weekday.index,
    y=weekday.values,
    title="Trips by Weekday"
)

st.plotly_chart(fig2, width='stretch')

df["hour"] = df["started_at"].dt.hour

# import plotly.express as px

fig = px.scatter_mapbox(
    df,
    lat="start_lat",
    lon="start_lng",
    animation_frame="hour",
    size_max=5,
    zoom=11,
    mapbox_style="carto-positron"
)

st.plotly_chart(fig)
