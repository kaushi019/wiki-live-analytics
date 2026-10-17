# app/pages/1_Polars_Analytics.py
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
st.markdown("This section reads operational KPIs and change-intent metrics computed dynamically by Polars from the `wikipedia_features` table.")

# Initialize our database helper connection class object
db = DBManager()
table_placeholder = st.empty()

# Continuous rendering tick thread loop
while True:
    conn = db.get_connection()
    try:
        # 🧠 UPGRADED SELECT: Pulling our newly extracted NLP intent tallies and experience scores
        query = """
            SELECT 
                page_title, 
                total_edits, 
                unique_users, 
                bot_edit_count, 
                total_bytes_changed, 
                ROUND(cast(avg_bytes_per_edit as numeric), 2) as avg_bytes_per_edit,
                typo_fixes_count, 
                vandalism_reversals_count, 
                ROUND(cast(avg_editor_experience as numeric), 0) as avg_editor_experience
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
            st.info("Waiting for the ingestor pipeline to save the first batch of Polars summaries... Keep your docker container cluster running!")
        else:
            # Highlight most edited pages inside an interactive UI data grid
            st.subheader("🔥 Top 50 Most Active Articles Across Stream Duration")
            
            # Format column configurations to look polished and human-readable
            st.dataframe(
                df_features.to_pandas(),
                width="stretch",
                hide_index=True,
                column_config={
                    "page_title": "Article Title",
                    "total_edits": "Total Edits",
                    "unique_users": "Unique Editors",
                    "bot_edit_count": "Bot Edits",
                    "total_bytes_changed": "Total Bytes Altered",
                    "avg_bytes_per_edit": "Avg Vol/Edit",
                    "typo_fixes_count": "✍️ Typo Fixes",
                    "vandalism_reversals_count": "🛡️ Vandalism Reversals",
                    "avg_editor_experience": "👤 Avg Editor Exp"
                }
            )
            
    # Sleep interval loop wait duration time (refreshes interface view every 3 seconds)
    time.sleep(3)
