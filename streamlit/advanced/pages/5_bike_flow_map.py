# # import streamlit as st
# # import pydeck as pdk
# # from utils.data_loader import load_data
# #
# # st.title("Bike Trip Flow Map")
# #
# # option = st.selectbox(
# #     "Select a year",
# #     (2020, 2021, 2022, 2023, 2024, 2025, 2026)
# # )
# # df = load_data(option)
# #
# # st.sidebar.title("Filters")
# #
# # date_range = st.sidebar.date_input(
# #     "Select Date Range",
# #     [df["date"].min(), df["date"].max()]
# # )
# #
# # user_type = st.sidebar.multiselect(
# #     "User Type",
# #     df["member_casual"].unique(),
# #     default=df["member_casual"].unique()
# # )
# #
# # bike_type = st.sidebar.multiselect(
# #     "Bike Type",
# #     df["rideable_type"].unique(),
# #     default=df["rideable_type"].unique()
# # )
# #
# # stations = st.sidebar.multiselect(
# #     "Start Stations",
# #     df["start_station_name"].dropna().unique()
# # )
# #
# # filtered_df = df[
# #     (df["member_casual"].isin(user_type)) &
# #     (df["rideable_type"].isin(bike_type))
# #     ]
# #
# # if len(date_range) == 2:
# #     filtered_df = filtered_df[
# #         (filtered_df["date"] >= date_range[0]) &
# #         (filtered_df["date"] <= date_range[1])
# #         ]
# #
# # if stations:
# #     filtered_df = filtered_df[
# #         filtered_df["start_station_name"].isin(stations)
# #     ]
# #
# # flow_df = filtered_df[[
# #     # "name",
# #     # "short_name",
# #     # "capacity",
# #     "start_lat",
# #     "start_lng",
# #     "end_lat",
# #     "end_lng"
# # ]].dropna()
# #
# # layer = pdk.Layer(
# #     "LineLayer",
# #     data=flow_df,  # limit for performance
# #     get_source_position="[start_lng, start_lat]",
# #     get_target_position="[end_lng, end_lat]",
# #     get_width=2,
# #     get_color=[0, 150, 255],
# #     pickable=True
# # )
# #
# # view_state = pdk.ViewState(
# #     latitude=38.9,
# #     longitude=-77.03,
# #     zoom=11
# # )
# #
# # tooltip = {
# #     "html": "<b>Station Name:</b> {name} <br/>"
# #             "<b>Short Name:</b> {short_name} <br/>"
# #             "<b>Capacity:</b> {capacity}",
# #     "style": {"backgroundColor": "steelblue", "color": "white"}
# # }
# #
# # selected_style = st.sidebar.selectbox(
# #     "Select Map Style",
# #     ["light", "dark", "satellite", "road"]
# # )
# # st.pydeck_chart(pdk.Deck(
# #     layers=[layer],
# #     map_style=selected_style,
# #     initial_view_state=view_state,
# #     tooltip=tooltip
# # ))
# #
# # layer_2 = pdk.Layer(
# #     "HeatmapLayer",
# #     data=df,
# #     get_position="[start_lng, start_lat]",
# #     aggregation="SUM"
# # )
# #
# # view_state = pdk.ViewState(
# #     latitude=38.9,
# #     longitude=-77.03,
# #     zoom=11
# # )
# # #
# # st.pydeck_chart(pdk.Deck(
# #     layers=[layer_2],
# #     initial_view_state=view_state,
# #     tooltip={"text": "Bike Trip"}
# # ))
#
#
# import streamlit as st
# import pydeck as pdk
#
# from utils.data_loader import load_data
# from utils.Ui import year_selector
#
# st.title("Bike Trip Flow Map")
#
# option = year_selector()
# df = load_data(option)
#
# # ── Sidebar Filters ───────────────────────────────────────────────────────────
#
# st.sidebar.title("Filters")
#
# date_range = st.sidebar.date_input(
#     "Select Date Range",
#     [df["date"].min(), df["date"].max()]
# )
#
# user_type = st.sidebar.multiselect(
#     "User Type",
#     df["member_casual"].unique(),
#     default=df["member_casual"].unique()
# )
#
# bike_type = st.sidebar.multiselect(
#     "Bike Type",
#     df["rideable_type"].unique(),
#     default=df["rideable_type"].unique()
# )
#
# stations = st.sidebar.multiselect(
#     "Start Stations",
#     df["start_station_name"].dropna().unique()
# )
#
# selected_style = st.sidebar.selectbox(
#     "Map Style",
#     ["light", "dark", "satellite", "road"]
# )
#
# # ── Apply Filters ─────────────────────────────────────────────────────────────
#
# filtered_df = df[
#     (df["member_casual"].isin(user_type)) &
#     (df["rideable_type"].isin(bike_type))
#     ].copy()
#
# if len(date_range) == 2:
#     filtered_df = filtered_df[
#         (filtered_df["date"] >= date_range[0]) &
#         (filtered_df["date"] <= date_range[1])
#         ]
#
# if stations:
#     filtered_df = filtered_df[
#         filtered_df["start_station_name"].isin(stations)
#     ]
#
# # ── Trip Flow Lines ───────────────────────────────────────────────────────────
#
# flow_df = filtered_df[[
#     "start_lat",
#     "start_lng",
#     "end_lat",
#     "end_lng",
#     "start_station_name",
# ]].dropna()
#
# line_layer = pdk.Layer(
#     "LineLayer",
#     data=flow_df,
#     get_source_position="[start_lng, start_lat]",
#     get_target_position="[end_lng, end_lat]",
#     get_width=2,
#     get_color=[0, 150, 255],
#     pickable=True
# )
#
# view_state = pdk.ViewState(latitude=38.9, longitude=-77.03, zoom=11)
#
# tooltip = {
#     "html": "<b>Origin:</b> {start_station_name}",
#     "style": {"backgroundColor": "steelblue", "color": "white"}
# }
#
# st.pydeck_chart(pdk.Deck(
#     layers=[line_layer],
#     map_style=selected_style,
#     initial_view_state=view_state,
#     tooltip=tooltip
# ))
#
# # ── Heatmap (uses filtered data) ──────────────────────────────────────────────
#
# st.subheader("Start Station Heatmap")
#
# heatmap_layer = pdk.Layer(
#     "HeatmapLayer",
#     data=filtered_df[["start_lat", "start_lng"]].dropna(),
#     get_position="[start_lng, start_lat]",
# )
#
# st.pydeck_chart(pdk.Deck(
#     layers=[heatmap_layer],
#     initial_view_state=view_state,
#     tooltip={"text": "Bike Trip"}
# ))

import streamlit as st
import pydeck as pdk

from utils.data_loader import load_data
from utils.Ui import year_selector

# ── CONFIG ───────────────────────────────────────────────────────────────────

MAX_ROWS = 5000  # safe limit for browser
TOP_STATIONS = 100  # avoid UI overload

st.set_page_config(layout="wide")
st.title("🚴 Bike Trip Flow Map")


# ── DATA LOADING (CACHED) ─────────────────────────────────────────────────────

@st.cache_data
def get_data(year):
    df = load_data(year)
    return df


year = year_selector()
df = get_data(year)

# ── SIDEBAR FILTERS ──────────────────────────────────────────────────────────

st.sidebar.title("Filters")

date_range = st.sidebar.date_input(
    "Date Range",
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

# Limit station list (IMPORTANT)
top_stations = (
    df["start_station_name"]
    .value_counts()
    .head(TOP_STATIONS)
    .index
)

stations = st.sidebar.multiselect(
    f"Start Stations (Top {TOP_STATIONS})",
    top_stations
)

map_style = st.sidebar.selectbox(
    "Map Style",
    ["light", "dark", "satellite", "road"]
)

mode = st.sidebar.radio(
    "Visualization Mode",
    ["Aggregated (Fast)", "Sampled (Detailed)"]
)

# ── FILTER DATA ──────────────────────────────────────────────────────────────

filtered_df = df[
    (df["member_casual"].isin(user_type)) &
    (df["rideable_type"].isin(bike_type))
].copy()

if len(date_range) == 2:
    filtered_df = filtered_df[
        (filtered_df["date"] >= date_range[0]) &
        (filtered_df["date"] <= date_range[1])
        ]

if stations:
    filtered_df = filtered_df[
        filtered_df["start_station_name"].isin(stations)
    ]

# ── VIEW STATE ───────────────────────────────────────────────────────────────

view_state = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11
)

# ── FLOW MAP ─────────────────────────────────────────────────────────────────

st.subheader("Trip Flow Map")

if mode == "Aggregated (Fast)":
    # GROUP trips → HUGE performance gain
    flow_df = filtered_df.groupby(
        ["start_lat", "start_lng", "end_lat", "end_lng"],
        as_index=False
    ).size().rename(columns={"size": "trip_count"})

    flow_df = flow_df.dropna()

    # optional safety cap
    if len(flow_df) > MAX_ROWS:
        flow_df = flow_df.nlargest(MAX_ROWS, "trip_count")

    line_layer = pdk.Layer(
        "LineLayer",
        data=flow_df,
        get_source_position="[start_lng, start_lat]",
        get_target_position="[end_lng, end_lat]",
        get_width="trip_count * 0.3",
        get_color=[0, 150, 255],
        pickable=True
    )

    tooltip = {
        "html": "<b>Trips:</b> {trip_count}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

else:
    # SAMPLE trips → controlled size
    flow_df = filtered_df[[
        "start_lat", "start_lng",
        "end_lat", "end_lng",
        "start_station_name"
    ]].dropna()

    if len(flow_df) > MAX_ROWS:
        flow_df = flow_df.sample(MAX_ROWS, random_state=42)

    line_layer = pdk.Layer(
        "LineLayer",
        data=flow_df,
        get_source_position="[start_lng, start_lat]",
        get_target_position="[end_lng, end_lat]",
        get_width=2,
        get_color=[0, 150, 255],
        pickable=True
    )

    tooltip = {
        "html": "<b>Origin:</b> {start_station_name}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

st.pydeck_chart(pdk.Deck(
    layers=[line_layer],
    map_style=map_style,
    initial_view_state=view_state,
    tooltip=tooltip
))

# ── HEATMAP ──────────────────────────────────────────────────────────────────

st.subheader("🔥 Start Location Heatmap")

heatmap_df = filtered_df[["start_lat", "start_lng"]].dropna()

if len(heatmap_df) > MAX_ROWS:
    heatmap_df = heatmap_df.sample(MAX_ROWS, random_state=42)

heatmap_layer = pdk.Layer(
    "HeatmapLayer",
    data=heatmap_df,
    get_position="[start_lng, start_lat]",
)

st.pydeck_chart(pdk.Deck(
    layers=[heatmap_layer],
    initial_view_state=view_state
))