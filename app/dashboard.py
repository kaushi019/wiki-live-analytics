# app/dashboard.py
import time
import streamlit as st
import polars as pl
from src.db_manager import DBManager

# 1. Page Configuration Setup
st.set_page_config(
    page_title="Wikipedia Live Stream AI Monitor",
    page_icon="📡",
    layout="wide"
)

st.title("📡 Wikipedia Live Stream AI Anomaly Monitor")
st.markdown("This dashboard updates automatically to display live insights captured from the Wikipedia API stream.")

# Initialize our database helper connection class object
db = DBManager()

def fetch_dashboard_data():
    """Queries recent raw streaming items directly into a Polars DataFrame."""
    query = """
        SELECT id, timestamp, page_title, user_name, bytes_changed, anomaly_score, ingested_at 
        FROM wikipedia_raw 
        ORDER BY id DESC 
        LIMIT 100;
    """
    conn = db.get_connection()
    try:
        df = pl.read_database(query=query, connection=conn)
        return df
    except Exception as e:
        st.error(f"Failed to fetch real-time metrics: {e}")
        return pl.DataFrame()
    finally:
        conn.close()

# 2. Establish a Live Loop Visual UI Container
# Storing placeholder references allows elements to be rewritten dynamically on tick intervals
metric_cards_placeholder = st.empty()
chart_placeholder = st.empty()

st.markdown("---")
st.subheader("🚨 Real-Time High-Score Anomaly Alerts (> 0.75)")
alert_table_placeholder = st.empty()

# Continuous rendering tick thread loop
while True:
    df_live = fetch_dashboard_data()
    
    if not df_live.is_empty():
        # --- SECTION A: HIGH-LEVEL KPI METRICS ---
        total_ingested = df_live.height
        high_anomalies_count = df_live.filter(pl.col("anomaly_score") > 0.75).height
        max_bytes = df_live["bytes_changed"].max()
        
        with metric_cards_placeholder.container():
            col1, col2, col3 = st.columns(3)
            col1.metric("Buffered Window Count", f"{total_ingested} records")
            col2.metric("AI Flagged Anomalies", f"{high_anomalies_count} caught", delta_color="inverse")
            col3.metric("Peak Size Variation", f"{max_bytes:,} bytes")
            
        # --- SECTION B: THE ANOMALY PLOT LINE CHART ---
        # Sort values chronologically so the chart reads correctly from left to right
        chart_data = df_live.sort("id", descending=False)
        
        with chart_placeholder.container():
            st.subheader("📈 Streaming AI Inference Anomaly Scores Over Time")
            # Map anomaly scores against the sequence index order
            st.line_chart(
                data=chart_data.to_pandas(),
                x="ingested_at",
                y="anomaly_score",
                use_container_width=True
            )
            
        # --- SECTION C: THE ALERTING PANEL DATAGRID TABLE ---
        # Filter down data view rows to target high-score outliers
        anomalies_df = df_live.filter(pl.col("anomaly_score") > 0.75).select([
            "anomaly_score", "page_title", "user_name", "bytes_changed", "ingested_at"
        ])
        
        with alert_table_placeholder.container():
            if anomalies_df.is_empty():
                st.info("Searching stream for suspicious activities... Current data profile is normal.")
            else:
                # Format dataframe table visually inside the frontend UI window canvas 
                st.dataframe(
                    anomalies_df.to_pandas(),
                    use_container_width=True,
                    hide_index=True
                )
                
    # Sleep interval loop wait duration time (refreshes interface view every 2 seconds)
    time.sleep(2)
