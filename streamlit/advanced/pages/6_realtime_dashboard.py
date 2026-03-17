import time

import pandas as pd
import pydeck as pdk
import requests

import streamlit as st


@st.cache_data(ttl=100)
def get_live_bikes():
    # GBFS_BASE = "https://gbfs.lyft.com/gbfs/2.3/bay/en"
    #
    # STATION_INFO_URL = f"{GBFS_BASE}/station_information.json"
    # url = "https://api.citybik.es/v2/networks/capital-bikeshare"
    url = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json"
    station_status_url = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json"

    station_response = requests.get(url=station_status_url).json()
    station_status = pd.DataFrame(station_response["data"]["stations"])
    response = requests.get(url)

    # stations = station_response.merge(
    #     response,
    #     on="station_id",
    #     how="left"
    # )
    data = response.json()

    stations = data["data"]["stations"]

    df_s = pd.DataFrame(stations)

    df_1 = df_s.merge(
        station_status,
        on="station_id",
        how="left"
    )

    return df_1


df = get_live_bikes()
# st.map(df[['lat','lon']])
# print(df.head())

st.sidebar.header("Filter Stations")
selected_stations = st.sidebar.multiselect(
    "Select Station Names",
    options=df['name'].unique(),
    default=df['name'].unique()
)

selected_style = st.sidebar.selectbox(
    "Select Map Style",
    ["light", "dark", "satellite", "road"]
)
# Apply filter to data
filtered_df = df[df['name'].isin(selected_stations)]

filtered_df['last_reported'] = pd.to_datetime(filtered_df['last_reported'], unit='s', utc=False)
filtered_df['last_reported'] = pd.to_datetime(filtered_df['last_reported'].astype('str'))

# Define tooltip
tooltip = {
    "html": "<b>Station Name:</b> {name} <br/>"
            "<b>Short Name:</b> {short_name} <br/>"
            "<b>Capacity:</b> {capacity} <br/>"
            "<b>Bikes Available:</b> {num_bikes_available} <br/>"
            "<b>E-bikes Available:</b> {num_ebikes_available} <br/>"
            "<b>Scooters Available:</b> {num_scooters_available} <br/>"
            "<b>Last Reported:</b> {last_reported} <br/>"
            "<b>Docks Available:</b> {num_docks_available} <br/>",

    "style": {"backgroundColor": "steelblue", "color": "white"}
}


# layer = pdk.Layer(
#     "ScatterplotLayer",
#     data=df,
#     get_position="[lon, lat]",
#     get_radius=50,
#     get_fill_color=[255,0,0],
#     pickable=True,  # This must be True for tooltips to work
# )

def get_color(val):
    if val > 10: return [255, 0, 0]  # Red
    if val > 20: return [0, 0, 0]  # Orange
    return [0, 255, 0]  # Green


# Add color column to your dataframe
filtered_df['color'] = filtered_df['capacity'].apply(get_color)

layer = pdk.Layer(
    "ColumnLayer",
    data=filtered_df,
    get_position=['lon', 'lat'],
    get_elevation='capacity',  # Column determining height
    elevation_scale=1,  # Adjust this to make bars taller/shorter
    radius=200,  # Width of the bars
    # get_fill_color="capacity > 20 ? [60, 0, 200, 140] : [0,200,100,160]",
    get_fill_color='color',
    pickable=True,
    extruded=True,  # Essential for 3D effect
    # get_fill_color="capacity > 500 ? [200, 30, 0, 160] : [0, 200, 100, 160]",
)

# Create deck.gl map
view_state = pdk.ViewState(
    latitude=filtered_df["lat"].mean(),
    longitude=filtered_df["lon"].mean(),
    zoom=10,
    pitch=45,  # Tilts the map
    bearing=0  # Rotates the map
)

r = pdk.Deck(
    map_style=selected_style,
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip
)

st.pydeck_chart(r)

placeholder = st.empty()
placeholder_2 = st.empty()
while True:
    live_trips = get_live_bikes()

    placeholder.metric(
        "Active Stations",
        len(live_trips)
    )
    placeholder_2.metric(
        "Last Reported",
        str(filtered_df['last_reported'].max())
    )

    time.sleep(100)
