import streamlit as st


def year_selector() -> int:
    """Shared year selectbox used across all pages."""
    return st.selectbox(
        "Select a year",
        (2020, 2021, 2022, 2023, 2024, 2025, 2026)
    )