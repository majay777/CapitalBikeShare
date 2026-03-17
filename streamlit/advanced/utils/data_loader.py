import pandas as pd
import pyarrow as pa


def load_data(year):
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
        f"tripdata/parquet/trips/year={year}/",
        filesystem=fs,
        format="parquet",
        partitioning="hive",
        schema=schema
    )

    table = dataset.to_table()

    df = table.to_pandas()

    # df = pd.read_csv("data/bikeshare_trips.csv")

    df["started_at"] = pd.to_datetime(df["started_at"])
    df["ended_at"] = pd.to_datetime(df["ended_at"])
    df['date'] = df['started_at'].dt.date
    df["hour"] = df["started_at"].dt.hour
    df["day"] = df["started_at"].dt.day_name()
    df["month"] = df["started_at"].dt.month

    df["duration_min"] = (
                                 df["ended_at"] - df["started_at"]
                         ).dt.total_seconds() / 60

    # st.sidebar.title("Filters")
    #
    # date_range = st.sidebar.date_input(
    # "Select Date Range",
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
    # filtered_df = df[
    #     (df["member_casual"].isin(user_type)) &
    #     (df["rideable_type"].isin(bike_type))
    #     ]
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

    return df
