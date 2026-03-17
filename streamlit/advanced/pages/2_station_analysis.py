import streamlit as st
import plotly.express as px
import pydeck as pdk
from utils.data_loader import load_data

option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)
df = load_data(option)

st.title("Station Analysis")

top = df["start_station_name"].value_counts().head(15)

fig = px.bar(
    x=top.values,
    y=top.index,
    orientation="h",
    title="Top Stations"
)

st.plotly_chart(fig, width='stretch')

st.subheader("Station Heatmap")

map_data = df[["start_lat", "start_lng"]].dropna()

layer = pdk.Layer(
    "HeatmapLayer",
    data=map_data,
    get_position="[start_lng, start_lat]"
)

view = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11
)

st.pydeck_chart(pdk.Deck(
    layers=[layer],
    initial_view_state=view
))

# routes = (
#     df.groupby([
#         "start_station_name",
#         "end_station_name"
#     ])
#     .size()
#     .reset_index(name="trip_count")
#     .sort_values("trip_count", ascending=False)
# )


# import networkx as nx
import plotly.graph_objects as go

# top_routes = routes.head(50)
#
# G = nx.from_pandas_edgelist(
#     top_routes,
#     "start_station_name",
#     "end_station_name",
#     edge_attr="trip_count"
# )
#
# pos = nx.spring_layout(G)
#
# edge_x = []
# edge_y = []
#
# for edge in G.edges():
#     x0,y0 = pos[edge[0]]
#     x1,y1 = pos[edge[1]]
#     edge_x += [x0,x1,None]
#     edge_y += [y0,y1,None]
#
# edge_trace = go.Scatter(
#     x=edge_x,
#     y=edge_y,
#     line=dict(width=1),
#     mode='lines'
# )
#
# node_x = []
# node_y = []
#
# for node in G.nodes():
#     x,y = pos[node]
#     node_x.append(x)
#     node_y.append(y)
#
# node_trace = go.Scatter(
#     x=node_x,
#     y=node_y,
#     mode='markers+text'
# )
#
# fig = go.Figure(data=[edge_trace,node_trace])
#
# st.plotly_chart(fig)


import pydeck as pdk

layer = pdk.Layer(
    "ArcLayer",
    data=df,
    get_source_position='[start_lng, start_lat]',
    get_target_position='[end_lng, end_lat]',
    get_width=2,
    get_source_color=[0, 128, 255],
    get_target_color=[255, 0, 80]
)

view_state = pdk.ViewState(
    latitude=38.9,
    longitude=-77.03,
    zoom=11,
    pitch=50
)
#
# deck = pdk.Deck(
#     layers=[layer],
#     initial_view_state=view_state
# )

# st.pydeck_chart(deck)


heat_df = df.groupby(
    ["hour", "start_lat", "start_lng"]
).size().reset_index(name="trips")

fig = px.density_mapbox(
    heat_df,
    lat="start_lat",
    lon="start_lng",
    z="trips",
    radius=15,
    animation_frame="hour",
    zoom=11,
    mapbox_style="carto-positron"
)

st.plotly_chart(fig)
