import pandas as pd
import plotly.express as px
import pyarrow as pa
import streamlit as st


st.set_page_config(
    page_title="Capital Bike Share Dashboard",
    layout="wide"

)

# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    h1, h2, h3 {
        font-family: 'Space Mono', monospace !important;
        letter-spacing: -0.5px;
    }
    .block-container {
        padding-top: 2rem;
    }
    .metric-card {
        background: #0f1117;
        border: 1px solid #2d2d3a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 1rem;
    }
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #00c9a7 !important;
        color: #000 !important;
        font-weight: 600;
        border-radius: 6px;
    }
    .year-badge {
        display: inline-block;
        background: #00c9a7;
        color: #000;
        font-family: 'Space Mono', monospace;
        font-size: 0.75rem;
        font-weight: 700;
        padding: 2px 10px;
        border-radius: 20px;
        margin: 2px;
    }
</style>
""", unsafe_allow_html=True)

available_years = sorted([2020, 2021, 2022, 2023, 2024, 2025, 2026])

# ── Multi-select year filter ──
st.markdown("### 📅 Filter by Year")

col_all, col_filter = st.columns([1, 4])

with col_all:
    select_all = st.checkbox("Select All", value=True)

with col_filter:
    if select_all:
        selected_years = st.multiselect(
            "Choose one or more years",
            options=available_years,
            default=available_years,
            key="year_filter",
        )
    else:
        selected_years = st.selectbox(
            "Choose one or more years",
            [2020, 2021, 2022, 2023, 2024, 2025, 2026],
            # default=available_years[0] if available_years else [],
            key="year_filter",
        )


# -----------------------------
# Load Data
# -----------------------------
@st.cache_data
def load_data():
    import pyarrow.dataset as ds
    import s3fs

    fs = s3fs.S3FileSystem(
        key="minioadmin",
        secret="minioadmin",
        client_kwargs={"endpoint_url": "http://localhost:9000"}
    )

    schema = pa.schema([
        ("ride_id", pa.string()),  # change to float
        ("rideable_type", pa.string()),
        ("started_at", pa.timestamp('ms')),
        ("ended_at", pa.timestamp('ms')),
        ("start_station_name", pa.string()),
        ("start_station_id", pa.string()),
        ("end_station_name", pa.string()),
        ("end_station_id", pa.string()),
        ("start_lat", pa.float64()),
        ("start_lng", pa.float64()),
        ("end_lat", pa.float64()),
        ("end_lng", pa.float64()),
        ("member_casual", pa.string())

    ])

    dataset = ds.dataset(
        f"tripdata/delta/trips/year={selected_years}/",
        filesystem=fs,
        format="parquet",
        partitioning="hive",
        schema=schema
    )

    table = dataset.to_table()

    df = table.to_pandas()

    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])

    df["date"] = df["started_at"].dt.date
    df["hour"] = df["started_at"].dt.hour
    df["weekday"] = df["started_at"].dt.day_name()

    df["trip_duration"] = (
                                  df["ended_at"] - df["started_at"]
                          ).dt.total_seconds() / 60

    return df


df = load_data()

# -----------------------------
# Sidebar Filters
# -----------------------------
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

# -----------------------------
# Filter Data
# -----------------------------
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

# -----------------------------
# Title
# -----------------------------
st.title("🚲 Capital Bike Share Dashboard – Washington DC")

st.markdown(
    """
    Interactive analytics dashboard for **Capital Bike Share trip data**.
    Explore trip patterns, station usage, and rider behavior.
    """
)

# -----------------------------
# KPI Metrics
# -----------------------------
st.subheader("Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Trips", f"{len(filtered_df):,}")

col2.metric(
    "Average Trip Duration (min)",
    round(filtered_df["trip_duration"].mean(), 2)
)

col3.metric(
    "Unique Start Stations",
    filtered_df["start_station_name"].nunique()
)

col4.metric(
    "Unique Bikes Used",
    filtered_df["ride_id"].nunique()
)

# -----------------------------
# Trips Over Time
# -----------------------------
st.subheader("Trips Over Time")

trips_by_day = (
    filtered_df
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

st.plotly_chart(fig, width='stretch')

# -----------------------------
# Hourly Demand Pattern
# -----------------------------
col1, col2 = st.columns(2)

with col1:
    hourly = (
        filtered_df
        .groupby("hour")
        .size()
        .reset_index(name="trips")
    )

    fig = px.bar(
        hourly,
        x="hour",
        y="trips",
        title="Trips by Hour"
    )

    st.plotly_chart(fig, width='stretch')

# -----------------------------
# Weekday Usage
# -----------------------------
with col2:
    weekday = (
        filtered_df
        .groupby("weekday")
        .size()
        .reset_index(name="trips")
    )

    fig = px.bar(
        weekday,
        x="weekday",
        y="trips",
        title="Trips by Weekday"
    )

    st.plotly_chart(fig, width='stretch')

# -----------------------------
# Top Stations
# -----------------------------
st.subheader("Top Start Stations")

top_stations = (
    filtered_df["start_station_name"]
    .value_counts()
    .head(10)
    .reset_index()
)

top_stations.columns = ["station", "trips"]

fig = px.bar(
    top_stations,
    x="station",
    y="trips",
    title="Top 10 Start Stations"
)

st.plotly_chart(fig, width='stretch')

# -----------------------------
# User Type Distribution
# -----------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Member vs Casual Riders")

    user_counts = (
        filtered_df["member_casual"]
        .value_counts()
        .reset_index()
    )

    user_counts.columns = ["user_type", "count"]

    fig = px.pie(
        user_counts,
        values="count",
        names="user_type"
    )

    st.plotly_chart(fig, width='stretch')

# -----------------------------
# Trip Duration Distribution
# -----------------------------
with col2:
    st.subheader("Trip Duration Distribution")

    fig = px.histogram(
        filtered_df,
        x="trip_duration",
        nbins=50
    )

    st.plotly_chart(fig, width='stretch')

# -----------------------------
# Map Visualization
# -----------------------------
st.subheader("Bike Station Map")

map_df = filtered_df[
    ["start_lat", "start_lng"]
].dropna()

map_df = map_df.rename(
    columns={
        "start_lat": "lat",
        "start_lng": "lon"
    }
)

st.map(map_df)

avg_duration = df.groupby("rideable_type")['trip_duration'].mean().reset_index()

fig = px.bar(avg_duration, x="rideable_type", y="trip_duration", color="rideable_type",
             title="Average Duration For Bikes")
st.plotly_chart(fig, width='stretch')

st.subheader("Top End Stations")

top_stations = (
    filtered_df["end_station_name"]
    .value_counts()
    .head(10)
    .reset_index()
)

top_stations.columns = ["station", "trips"]

fig = px.bar(
    top_stations,
    x="station",
    y="trips",
    title="Top 10 End Stations"
)

st.plotly_chart(fig, width='stretch')

# -----------------------------
# Raw Data Explorer
# -----------------------------
# st.subheader("Trip Data Explorer")
#
# st.dataframe(filtered_df)
