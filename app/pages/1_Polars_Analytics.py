import time
import streamlit as st
import polars as pl
from src.db_manager import DBManager

# 1. Page Configuration Setup
st.set_page_config(
    page_title="Polars Aggregations Analytics", 
    page_icon="⚙️", 
    layout="wide"
)

st.title("⚙️ Polars Real-Time Aggregated Feature Insights")
st.markdown("This section reads historical snapshots computed directly by Polars from the `wikipedia_features` table during ingestion batch cycles.")

# Initialize our database helper connection class object
db = DBManager()
table_placeholder = st.empty()

# Continuous rendering tick thread loop
while True:
    conn = db.get_connection()
    try:
        # Pull features calculated during our ingestor batch loop
        query = """
            SELECT page_title, total_edits, unique_users, bot_edit_count, total_bytes_changed, avg_bytes_per_edit 
            FROM wikipedia_features 
            ORDER BY total_edits DESC 
            LIMIT 50;
        """
        df_features = pl.read_database(query=query, connection=conn)
    except Exception as e:
        st.error(f"Failed to load analytics from database: {e}")
        df_features = pl.DataFrame()
    finally:
        conn.close()

    with table_placeholder.container():
        if df_features.is_empty():
            st.info("Waiting for the ingestor pipeline to save the first batch of Polars summaries... Keep run.py running!")
        else:
            # Highlight most edited pages inside an interactive UI data grid
            st.subheader("🔥 Top 50 Most Heavily Modified Pages Across Stream Duration")
            st.dataframe(
                df_features.to_pandas(),
                use_container_width=True,
                hide_index=True
            )
            
    # Sleep interval loop wait duration time (refreshes interface view every 3 seconds)
    time.sleep(3)
