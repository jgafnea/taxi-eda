import os

import duckdb
import streamlit as st
import streamlit_shadcn_ui as ui
from dotenv import find_dotenv, load_dotenv

# Get variables from .env in parent directory
load_dotenv(find_dotenv())


PARQUET_URL = os.getenv("PARQUET_URL")
con = duckdb.connect(database=":memory:")

st.set_page_config(layout="centered")

# Query hourly trends
query = """
SELECT
    COUNT(*) AS total_trips,
    SUM(fare_amount + tip_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour
    -- ? is replaced below by the value of PARQUET_URL at runtime
FROM read_parquet(?)
GROUP BY hour
ORDER BY hour;
"""
df = con.execute(query, [PARQUET_URL]).df()

# Compute overall metrics; necessary because the SQL `GROUP BY`
# returns one row per group (per hour), not a single total.
total_revenue = df["total_revenue"].sum()
total_trips = df["total_trips"].sum()
avg_fare = df["avg_fare"].mean()

st.header("Dashboard")

# Metric cards
cols = st.columns(3)
with cols[0]:
    ui.metric_card(title="Total Revenue", content=f"${total_revenue:,.0f}", key="card1")
with cols[1]:
    ui.metric_card(title="Total Trips", content=f"{total_trips:,}", key="card2")
with cols[2]:
    ui.metric_card(title="Average Fare", content=f"${avg_fare:.2f}", key="card3")

# Hourly chart
with st.container(border=True):
    st.vega_lite_chart(
        df,
        {
            "mark": {
                "type": "bar",
                "tooltip": True,
                "fill": "rgb(255, 205, 86)",  # taxi-ish yellow
                "cornerRadiusEnd": 4,
            },
            "encoding": {
                "x": {"field": "hour", "type": "ordinal", "title": "Hour of Day"},
                "y": {"field": "total_trips", "type": "quantitative", "title": "Trips"},
            },
            "config": {
                "view": {"stroke": None},
                "axis": {"labelFontSize": 12, "titleFontSize": 13},
            },
        },
        width="content",
    )

# SQL
with st.expander("Show SQL"):
    st.code(query, language="sql")
