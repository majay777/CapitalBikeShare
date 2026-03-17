import streamlit as st

st.set_page_config(
    page_title="Capital Bike Share Analytics",
    page_icon="🚲",
    layout="wide"
)# ─────────────────────────────────────────────
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



st.title("🚲 Capital Bike Share Advanced Analytics Dashboard")

st.markdown("""
This dashboard provides advanced analytics for **Washington DC Capital Bike Share data**.

### Features
• Trip demand analysis  
• Station popularity insights  
• Interactive maps  
• Rider behavior analytics  
• Demand prediction using Machine Learning
""")

st.sidebar.success("Select a page above.")
