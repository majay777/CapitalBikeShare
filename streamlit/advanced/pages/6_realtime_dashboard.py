import time
import logging
from typing import Optional

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

REFRESH_INTERVAL = 60  # seconds


@st.cache_data(ttl=REFRESH_INTERVAL)
def get_live_bikes() -> Optional[pd.DataFrame]:
    """Fetch live bike station data with error handling."""
    station_info_url = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json"
    station_status_url = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json"

    try:
        # Add timeout and headers for robustness
        headers = {
            'User-Agent': 'Capital-Bike-Share-Dashboard/1.0'
        }
        
        info_response = requests.get(station_info_url, headers=headers, timeout=10)
        info_response.raise_for_status()
        info_data = info_response.json()
        
        status_response = requests.get(station_status_url, headers=headers, timeout=10)
        status_response.raise_for_status()
        status_data = status_response.json()
        
        df_info = pd.DataFrame(info_data["data"]["stations"])
        df_status = pd.DataFrame(status_data["data"]["stations"])
        
        return df_info.merge(df_status, on="station_id", how="left")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        st.error(f"Failed to fetch station data: {str(e)}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"Data parsing failed: {e}")
        st.error(f"Failed to parse station data: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        st.error(f"An unexpected error occurred: {str(e)}")
        return None


# Get data with error handling
df = get_live_bikes()
if df is None:
    st.stop()  # Stop execution if data fetch failed

# ── Sidebar ───────────────────────────────────────────────────────────────────

st.sidebar.header("Filter Stations")

selected_stations = st.sidebar.multiselect(
    "Select Station Names",
    options=df["name"].unique(),
    default=df["name"].unique()
)

selected_style = st.sidebar.selectbox(
    "Map Style",
    ["light", "dark", "satellite", "road"]
)

# ── Filter ────────────────────────────────────────────────────────────────────

# Use .copy() to avoid SettingWithCopyWarning on subsequent mutations
filtered_df = df[df["name"].isin(selected_stations)].copy()

filtered_df["last_reported"] = pd.to_datetime(
    filtered_df["last_reported"], unit="s", utc=False
)

# ── Color by capacity ─────────────────────────────────────────────────────────

def get_color(val: int) -> list:
    """Return RGB color based on station capacity.
    Check higher threshold first to avoid unreachable branches.
    """
    if val > 20:
        return [255, 165, 0]   # Orange — large station
    elif val > 10:
        return [255, 0, 0]     # Red — medium station
    else:
        return [0, 255, 0]         # Green — small station


filtered_df["color"] = filtered_df["capacity"].apply(get_color)

# ── Map ───────────────────────────────────────────────────────────────────────

st.title("Real-Time Station Dashboard")

tooltip = {
    "html": (
        "<b>Station:</b> {name}<br/>"
        "<b>Short Name:</b> {short_name}<br/>"
        "<b>Capacity:</b> {capacity}<br/>"
        "<b>Bikes Available:</b> {num_bikes_available}<br/>"
        "<b>E-bikes Available:</b> {num_ebikes_available}<br/>"
        "<b>Scooters Available:</b> {num_scooters_available}<br/>"
        "<b>Docks Available:</b> {num_docks_available}<br/>"
        "<b>Last Reported:</b> {last_reported}"
    ),
    "style": {"backgroundColor": "steelblue", "color": "white"}
}

layer = pdk.Layer(
    "ColumnLayer",
    data=filtered_df,
    get_position=["lon", "lat"],
    get_elevation="capacity",
    elevation_scale=1,
    radius=200,
    get_fill_color="color",
    pickable=True,
    extruded=True,
)

view_state = pdk.ViewState(
    latitude=filtered_df["lat"].mean(),
    longitude=filtered_df["lon"].mean(),
    zoom=10,
    pitch=45,
    bearing=0
)

st.pydeck_chart(pdk.Deck(
    map_style=selected_style,
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip
))

# ── Metrics ───────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)
col1.metric("Active Stations", len(filtered_df))
col2.metric("Last Reported", str(filtered_df["last_reported"].max()))

# ── Auto-refresh ──────────────────────────────────────────────────────────────
# Use session state for controlled refresh to avoid blocking
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

# Check if it's time to refresh
current_time = time.time()
if current_time - st.session_state.last_refresh >= REFRESH_INTERVAL:
    st.session_state.last_refresh = current_time
    st.rerun()
else:
    # Show countdown to next refresh
    time_until_refresh = int(REFRESH_INTERVAL - (current_time - st.session_state.last_refresh))
    st.caption(f"Auto-refresh in {time_until_refresh} seconds")