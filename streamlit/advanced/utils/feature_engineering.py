def create_features(df):
    df["is_weekend"] = df["day"].isin(
        ["Saturday", "Sunday"]
    ).astype(int)

    df["rush_hour"] = df["hour"].isin(
        [7, 8, 9, 16, 17, 18]
    ).astype(int)

    return df
