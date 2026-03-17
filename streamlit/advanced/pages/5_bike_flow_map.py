import streamlit as st
import pydeck as pdk
from utils.data_loader import load_data

st.title("Bike Trip Flow Map")

option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)
df = load_data(option)

st.sidebar.title("Filters")

date_range = st.sidebar.date_input(
    "Select Date Range",
    [df["date"].min(), df["date"].max()]
)

user_type = st.sidebar.multiselect(
    "User Type",
    df["member_casual"].unique(),
    default=df["member_casual"].unique()
)

bike_type = st.sidebar.multiselect(
    "Bike Type",
    df["rideable_type"].unique(),
    default=df["rideable_type"].unique()
)

stations = st.sidebar.multiselect(
    "Start Stations",
    df["start_station_name"].dropna().unique()
)

filtered_df = df[
    (df["member_casual"].isin(user_type)) &
    (df["rideable_type"].isin(bike_type))
    ]

if len(date_range) == 2:
    filtered_df = filtered_df[
        (filtered_df["date"] >= date_range[0]) &
        (filtered_df["date"] <= date_range[1])
        ]

if stations:
    filtered_df = filtered_df[
        filtered_df["start_station_name"].isin(stations)
    ]

flow_df = filtered_df[[
    # "name",
    # "short_name",
    # "capacity",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng"
]].dropna()

layer = pdk.Layer(
    "LineLayer",
    data=flow_df,  # limit for performance
    get_source_position="[start_lng, start_lat]",
    get_target_position="[end_lng, end_lat]",
    get_width=2,
    get_color=[0, 150, 255],
    pickable=True
)

view_state = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11
)

tooltip = {
    "html": "<b>Station Name:</b> {name} <br/>"
            "<b>Short Name:</b> {short_name} <br/>"
            "<b>Capacity:</b> {capacity}",
    "style": {"backgroundColor": "steelblue", "color": "white"}
}

selected_style = st.sidebar.selectbox(
    "Select Map Style",
    ["light", "dark", "satellite", "road"]
)
st.pydeck_chart(pdk.Deck(
    layers=[layer],
    map_style=selected_style,
    initial_view_state=view_state,
    tooltip=tooltip
))

layer_2 = pdk.Layer(
    "HeatmapLayer",
    data=df,
    get_position="[start_lng, start_lat]",
    aggregation="SUM"
)

view_state = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11
)
#
st.pydeck_chart(pdk.Deck(
    layers=[layer_2],
    initial_view_state=view_state,
    tooltip={"text": "Bike Trip"}
))
