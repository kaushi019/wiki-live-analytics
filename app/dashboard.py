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
st.markdown("Real-time operational risk dashboard pulling streaming telemetry feeds from Apache Kafka message queues.")

# Initialize database connection helper
db = DBManager()

def fetch_dashboard_data():
    """Queries recent raw streaming items directly into a Polars DataFrame."""
    query = """
        SELECT 
            id, 
            timestamp, 
            COALESCE(page_title, '') AS page_title, 
            COALESCE(user_name, '') AS user_name, 
            is_bot, 
            bytes_changed, 
            COALESCE(edit_comment, '') AS edit_comment, 
            is_minor, 
            user_edit_count, 
            anomaly_score, 
            ingested_at 
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

# 2. Establish Permanent High-Level Layout Containers
metric_cards_placeholder = st.empty()
st.markdown("---")

# Main split-screen structural layout canvas placeholder
split_screen_placeholder = st.empty()

# Continuous rendering loop
while True:
    df_live = fetch_dashboard_data()
    
    if not df_live.is_empty():
        # Calculate key summary variables
        total_ingested = df_live.height
        high_anomalies_df = df_live.filter(pl.col("anomaly_score") > 0.75)
        high_anomalies_count = high_anomalies_df.height
        max_bytes = df_live["bytes_changed"].max()
        
        # --- SECTION A: HIGH-LEVEL KPI METRICS (Always visible at the top) ---
        with metric_cards_placeholder.container():
            col1, col2, col3 = st.columns(3)
            col1.metric("Buffered Window Count", f"{total_ingested} records")
            col2.metric("AI Flagged Anomalies", f"{high_anomalies_count} caught", delta_color="inverse")
            col3.metric("Peak Size Variation", f"{max_bytes:,} bytes")
            
        # --- SECTION B: DUAL SPLIT-SCREEN VIEW CANVAS ---
        with split_screen_placeholder.container():
            # Create a 60/40 horizontal split on the main dashboard screen
            left_view, right_view = st.columns([3, 2], gap="large")
            
            # 📈 LEFT SIDE: CORE VISUALIZATIONS (60% Width)
            with left_view:
                st.subheader("📊 Streaming AI Anomaly Scores Over Time")
                chart_data = df_live.sort("id", descending=False)
                st.line_chart(
                    data=chart_data.to_pandas(),
                    x="ingested_at",
                    y="anomaly_score",
                    width="stretch"
                )
                
                st.subheader("📋 Recent High-Score Window Alerts")
                anomalies_display = high_anomalies_df.select([
                    "anomaly_score", "page_title", "user_name", "bytes_changed"
                ])
                if anomalies_display.is_empty():
                    st.info("Searching stream for suspicious activities... Current data profile is normal.")
                else:
                    st.dataframe(anomalies_display.to_pandas(), width="stretch", hide_index=True)
            
            # 🛡️ RIGHT SIDE: IMMEDIATE REAL-TIME SLA TICKETS (40% Width - Always In Front!)
            with right_view:
                st.subheader(f"⚡ Live SLA Incident Logs ({high_anomalies_count} Active)")
                sla_breaches = high_anomalies_df.head(4) # Target top 4 most recent critical incidents
                
                if sla_breaches.is_empty():
                    st.success("✅ Operational SLAs Intact. No high-priority incidents logged.")
                else:
                    # Enclose in a clean container scroll wrapper box for aesthetic spacing
                    with st.container():
                        for row in sla_breaches.iter_rows(named=True):
                            score = row["anomaly_score"]
                            exp = row["user_edit_count"]
                            is_bot = row["is_bot"]
                            page = row["page_title"]
                            user = row["user_name"]
                            bytes_mod = row["bytes_changed"]
                            
                            # P1 CRITICAL: High Anomaly Score + Sleeper/New Account
                            if score > 0.85 and exp < 10:
                                st.error(
                                    f"🚨 **[P1 CRITICAL] SECURITY BREACH**\n\n"
                                    f"Sleeper account **'{user}'** ({exp} edits) made an extreme change of **{bytes_mod:,} bytes** "
                                    f"on article *'{page}'*.\n\n"
                                    f"**Operational Routing:** Account flagged for suspension."
                                )
                            # P2 HIGH: High Anomaly Score + Rogue System Bot Infrastructure
                            elif score > 0.75 and is_bot:
                                st.warning(
                                    f"🤖 **[P2 HIGH] AUTOMATION INSTABILITY**\n\n"
                                    f"Automated bot **'{user}'** triggered behavior risk anomaly scoring **{score:.2f}** "
                                    f"on article *'{page}'*.\n\n"
                                    f"**Operational Routing:** Rate-limiting throttle applied."
                                )
                            # P3 MEDIUM: Content Outlier Vandalism Deviation
                            else:
                                st.info(
                                    f"📝 **[P3 MEDIUM] CONTENT INTEGRITY**\n\n"
                                    f"High-volume layout modification of **{bytes_mod:,} bytes** by editor **'{user}'** "
                                    f"on article *'{page}'*.\n\n"
                                    f"**Operational Routing:** Routed to human moderation queue."
                                )
                            st.markdown("---")
                        
    # Refresh interface view every 2 seconds
    time.sleep(2)
