import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from typing import Optional

st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)


def load_data(status_filter: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    base_query = "SELECT * FROM ride_trips"
    params = {}

    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter

    base_query += " ORDER BY trip_id DESC LIMIT :limit"
    params["limit"] = limit

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(base_query), conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Sidebar
status_options = ["All", "Requested", "Accepted", "In Progress", "Completed", "Cancelled"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input("Number of records to load", min_value=50, max_value=2000, value=200, step=50)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()


# MAIN LOOP
while True:
    df_trips = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("No trip records found. Waiting for data…")
            time.sleep(update_interval)
            continue

        # Convert timestamp
        if "timestamp" in df_trips.columns:
            df_trips["timestamp"] = pd.to_datetime(df_trips["timestamp"])

        # Core KPIs
        total_trips = len(df_trips)
        avg_fare = df_trips["fare"].mean()
        avg_distance = df_trips["distance_km"].mean()
        completed = len(df_trips[df_trips["status"] == "Completed"])
        cancelled = len(df_trips[df_trips["status"] == "Cancelled"])
        completed_rate = (completed / total_trips * 100) if total_trips > 0 else 0.0

        # 🔥 NEW KPIs
        # Estimated average trip duration
        avg_speed_kmh = 35
        avg_duration_min = (df_trips["distance_km"] / avg_speed_kmh * 60).mean()

        # Most popular ride type
        most_popular_ride = df_trips["ride_type"].mode()[0]

        # Top revenue city
        top_city = df_trips.groupby("city")["fare"].sum().idxmax()

        st.subheader(f"Displaying {total_trips} trips (Filter: {selected_status})")

        # Display KPIs
        k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
        k1.metric("Total Trips", total_trips)
        k2.metric("Avg Fare", f"${avg_fare:,.2f}")
        k3.metric("Avg Distance (km)", f"{avg_distance:,.2f}")
        k4.metric("Completion Rate", f"{completed_rate:,.2f}%")
        k5.metric("Cancelled", cancelled)
        k6.metric("Est. Avg Duration (min)", f"{avg_duration_min:.1f}")
        k7.metric("Top Revenue City", top_city)

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_trips.head(10), use_container_width=True)

        # ========== EXISTING CHARTS ==========
        fare_by_type = df_trips.groupby("ride_type")["fare"].mean().reset_index().sort_values("fare", ascending=False)
        fig_ride_type = px.bar(fare_by_type, x="ride_type", y="fare", title="Average Fare by Ride Type")

        trips_by_city = df_trips.groupby("city")["trip_id"].count().reset_index().rename(columns={"trip_id": "count"})
        fig_city = px.pie(trips_by_city, values="count", names="city", title="Trip Volume by City")

        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(fig_ride_type, use_container_width=True)
        with col2:
            st.plotly_chart(fig_city, use_container_width=True)

        # ========== NEW VISUALIZATIONS ==========

        st.markdown("## Additional Insights")

        # 1. Fare distribution
        fig_fare_dist = px.histogram(
            df_trips,
            x="fare",
            nbins=30,
            title="Fare Distribution"
        )

        # 2. Distance vs Fare
        fig_scatter = px.scatter(
            df_trips,
            x="distance_km",
            y="fare",
            trendline="ols",
            title="Distance vs Fare",
            labels={"distance_km": "Distance (km)", "fare": "Fare ($)"}
        )

        colA, colB = st.columns(2)
        with colA:
            st.plotly_chart(fig_fare_dist, use_container_width=True)
        with colB:
            st.plotly_chart(fig_scatter, use_container_width=True)

        # 3. Trips over time by status
        df_time = df_trips.copy()
        df_time["minute"] = df_time["timestamp"].dt.floor("T")
        status_counts = df_time.groupby(["minute", "status"])["trip_id"].count().reset_index()

        fig_status_time = px.line(
            status_counts,
            x="minute",
            y="trip_id",
            color="status",
            title="Trips Over Time by Status",
            labels={"trip_id": "Trip Count"}
        )

        # 4. Revenue by hour of day
        df_hour = df_trips.copy()
        df_hour["hour"] = df_hour["timestamp"].dt.hour
        revenue_hour = df_hour.groupby("hour")["fare"].sum().reset_index()

        fig_revenue_hour = px.bar(
            revenue_hour,
            x="hour",
            y="fare",
            title="Revenue by Hour of Day"
        )

        colC, colD = st.columns(2)
        with colC:
            st.plotly_chart(fig_status_time, use_container_width=True)
        with colD:
            st.plotly_chart(fig_revenue_hour, use_container_width=True)

        st.caption(f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s")

    time.sleep(update_interval)
