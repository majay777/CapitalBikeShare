import streamlit as st
import pandas as pd
from utils.data_loader import load_data

st.title("City Mobility Analytics")

option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)
df = load_data(option)

df["hour"] = df["started_at"].dt.hour

flow_df = (
    df.groupby([
        "hour",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng"
    ])
    .size()
    .reset_index(name="trip_count")
)

import plotly.express as px
import streamlit as st

# fig = px.scatter_mapbox(
#     flow_df,
#     lat="start_lat",
#     lon="start_lng",
#     animation_frame="hour",
#     size="trip_count",
#     zoom=11,
#     mapbox_style="carto-positron",
#     title="Citywide Bike Flow Animation"
# )
#
# st.plotly_chart(fig, use_container_width=True)

import pydeck as pdk

layer = pdk.Layer(
    "ArcLayer",
    data=flow_df,
    get_source_position='[start_lng, start_lat]',
    get_target_position='[end_lng, end_lat]',
    get_width="trip_count",
    get_source_color=[0, 128, 255],
    get_target_color=[255, 80, 80]
)

view_state = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11,
    pitch=45
)

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view_state
))

od_matrix = pd.crosstab(
    df["start_station_name"],
    df["end_station_name"]
)

import plotly.express as px

fig = px.imshow(
    od_matrix,
    color_continuous_scale="Blues",
    title="Origin-Destination Matrix"
)

st.plotly_chart(fig, width='stretch')

top_routes = (
    df.groupby([
        "start_station_name",
        "end_station_name"
    ])
    .size()
    .reset_index(name="trip_count")
    .sort_values("trip_count", ascending=False)
    .head(20)
)

st.dataframe(top_routes)

station = st.selectbox(
    "Select Origin Station",
    df["start_station_name"].unique()
)

filtered = df[df["start_station_name"] == station]

od_filtered = pd.crosstab(
    filtered["start_station_name"],
    filtered["end_station_name"]
)

st.dataframe(od_filtered)

# tab1, tab2 = st.tabs([
#     "Citywide Flow Animation",
#     "Origin-Destination Matrix"
# ])
