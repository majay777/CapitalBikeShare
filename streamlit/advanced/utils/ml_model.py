from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split


def train_model(df):
    demand = df.groupby(["hour"]).size().reset_index(name="trips")

    X = demand[["hour"]]
    y = demand["trips"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2
    )

    model = RandomForestRegressor()

    model.fit(X_train, y_train)

    return model
