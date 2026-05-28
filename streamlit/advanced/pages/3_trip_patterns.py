# import streamlit as st
# import plotly.express as px
# from utils.data_loader import load_data
#
# option = st.selectbox(
#     "Select a year",
#     (2020, 2021, 2022, 2023, 2024, 2025, 2026)
# )
#
# df = load_data(option)
#
# st.title("Trip Patterns")
#
# hourly = df.groupby("hour").size()
#
# fig = px.bar(
#     x=hourly.index,
#     y=hourly.values,
#     labels={"x": "Hour", "y": "Trips"}
# )
#
# st.plotly_chart(fig, width='stretch')
#
# weekday = df.groupby("day").size()
#
# fig2 = px.bar(
#     x=weekday.index,
#     y=weekday.values,
#     title="Trips by Weekday"
# )
#
# st.plotly_chart(fig2, width='stretch')
#
# df["hour"] = df["started_at"].dt.hour
#
# # import plotly.express as px
#
# fig = px.scatter_mapbox(
#     df,
#     lat="start_lat",
#     lon="start_lng",
#     animation_frame="hour",
#     size_max=5,
#     zoom=11,
#     mapbox_style="carto-positron"
# )
#
# st.plotly_chart(fig)

import streamlit as st
import plotly.express as px

from utils.data_loader import load_data
from utils.Ui import year_selector

option = year_selector()
df = load_data(option)

st.title("Trip Patterns")

# ── Trips by Hour ─────────────────────────────────────────────────────────────

hourly = df.groupby("hour").size().reset_index(name="trips")

fig = px.bar(
    hourly,
    x="hour",
    y="trips",
    title="Trips by Hour of Day",
    labels={"hour": "Hour", "trips": "Trips"}
)
st.plotly_chart(fig, use_container_width=True)

# ── Trips by Weekday ──────────────────────────────────────────────────────────

weekday = df.groupby("day").size().reset_index(name="trips")

fig = px.bar(
    weekday,
    x="day",
    y="trips",
    title="Trips by Weekday",
    labels={"day": "Day", "trips": "Trips"}
)
st.plotly_chart(fig, use_container_width=True)

# ── Animated Map: Trips by Hour ───────────────────────────────────────────────

st.subheader("Trip Origins by Hour")

# Pre-aggregate to a grid before passing to Plotly — raw trip rows are too large
map_grid = (
    df
    .assign(
        lat=df["start_lat"].round(3),
        lng=df["start_lng"].round(3)
    )
    .groupby(["hour", "lat", "lng"])
    .size()
    .reset_index(name="count")
)

fig = px.scatter_mapbox(
    map_grid,
    lat="lat",
    lon="lng",
    size="count",
    animation_frame="hour",
    size_max=15,
    zoom=11,
    mapbox_style="carto-positron",
    title="Trip Origins by Hour"
)
st.plotly_chart(fig, use_container_width=True)
