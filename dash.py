import duckdb
import plotly.express as px
import streamlit as st

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PARQUET_URL = f"{BASE_URL}/yellow_tripdata_2024-01.parquet"

st.set_page_config(page_title="NYC Taxi Trips", layout="centered")

# Header; use markdown to center
st.markdown(
    "<div style='text-align: center;'>"
    "<h1>Taxi Patterns</h1>"
    "<h3>Hourly trip density by day of week — January 2024</h3>"
    "</div>",
    unsafe_allow_html=True,
)

# start_time = time.time()
con = duckdb.connect(database=":memory:")

query = """
    SELECT
        EXTRACT(DOW FROM tpep_pickup_datetime) AS pickup_day,
        EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
        COUNT(*) AS trips
    -- DuckDB replaces ? with the value of PARQUET_URL
    FROM read_parquet(?)
    GROUP BY 1, 2
    ORDER BY 1, 2
"""
df = con.execute(query, [PARQUET_URL]).df()

# Clean up day names and order
day_map = {
    0: "Sunday",
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
}
df["pickup_day"] = df["pickup_day"].map(day_map)
order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Pivot for heatmap
heatmap_data = df.pivot(index="pickup_day", columns="pickup_hour", values="trips")
heatmap_data = heatmap_data.reindex(order)

# Make smaller, neutral heatmap
fig = px.imshow(
    heatmap_data,
    # color_continuous_scale="Viridis",
    labels={"x": "Hour of Day", "y": "", "color": "Trips"},
    aspect="auto",
)

st.plotly_chart(fig)

# Show the query
st.write("Query:")
st.code(query, language="sql")

# elapsed = time.time() - start_time
# st.caption(f"Processed in {elapsed:.2f} seconds")
