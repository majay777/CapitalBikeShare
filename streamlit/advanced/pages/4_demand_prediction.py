# import streamlit as st
# import pandas as pd
# import plotly.express as px
#
# from utils.data_loader import load_data
# from utils.ml_model import train_model
#
# option = st.selectbox(
#     "Select a year",
#     (2020, 2021, 2022, 2023, 2024, 2025, 2026)
# )
# df = load_data(option)
#
# st.title("Demand Prediction")
#
# model = train_model(df)
#
# future_hours = pd.DataFrame({
#     "hour": list(range(24))
# })
#
# future_hours["predicted_trips"] = model.predict(future_hours)
#
# fig = px.line(
#     future_hours,
#     x="hour",
#     y="predicted_trips",
#     title="Predicted Bike Demand by Hour"
# )
#
# st.plotly_chart(fig, width='stretch')


import streamlit as st
import pandas as pd
import plotly.express as px

from utils.data_loader import load_data
from utils.ml_model import train_model
from utils.Ui import year_selector

option = year_selector()

st.title("Demand Prediction")


# Cache the trained model keyed on the selected year so it doesn't retrain
# on every widget interaction — only when the year actually changes.
@st.cache_resource
def get_model(year):
    data = load_data(year)
    return train_model(data)


model = get_model(option)

future_hours = pd.DataFrame({"hour": list(range(24))})
future_hours["predicted_trips"] = model.predict(future_hours)

fig = px.line(
    future_hours,
    x="hour",
    y="predicted_trips",
    title="Predicted Bike Demand by Hour",
    labels={"hour": "Hour of Day", "predicted_trips": "Predicted Trips"}
)
st.plotly_chart(fig, use_container_width=True)
