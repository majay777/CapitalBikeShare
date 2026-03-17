import streamlit as st
import pandas as pd
import plotly.express as px

from utils.data_loader import load_data
from utils.ml_model import train_model

option = st.selectbox(
    "Select a year",
    (2020, 2021, 2022, 2023, 2024, 2025, 2026)
)
df = load_data(option)

st.title("Demand Prediction")

model = train_model(df)

future_hours = pd.DataFrame({
    "hour": list(range(24))
})

future_hours["predicted_trips"] = model.predict(future_hours)

fig = px.line(
    future_hours,
    x="hour",
    y="predicted_trips",
    title="Predicted Bike Demand by Hour"
)

st.plotly_chart(fig, width='stretch')
