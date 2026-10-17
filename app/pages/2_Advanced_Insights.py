# app/pages/2_Advanced_Insights.py
import streamlit as st
import polars as pl
import altair as alt  # 📊 Layered charts with exact count placeholders
from src.db_manager import DBManager

# 1. Page Configuration Setup
st.set_page_config(
    page_title="Advanced Insights Console", 
    page_icon="🔍", 
    layout="wide"
)

st.title("🔍 Advanced Business Insights & Search Console")
st.markdown("Query rich text fields, track change intent, and audit editor experience statistics across the streaming lifecycle.")

# Initialize our database helper connection class object
db = DBManager()

# Connect to database to load the historical context frame
conn = db.get_connection()
try:
    query = """
        SELECT 
            COALESCE(page_title, '') AS page_title, 
            COALESCE(user_name, '') AS user_name, 
            is_bot, 
            bytes_changed, 
            COALESCE(edit_comment, '') AS edit_comment, 
            is_minor, 
            user_edit_count, 
            anomaly_score, 
            ingested_at 
        FROM wikipedia_raw;
    """
    df = pl.read_database(query=query, connection=conn)
except Exception as e:
    st.error(f"Failed to query backend database tables: {e}")
    df = pl.DataFrame()
finally:
    conn.close()

if df.is_empty():
    st.warning("📭 Database table is currently empty. Run your cluster backend (`docker compose up --build`) to collect multi-dimensional records!")
else:
    # -------------------------------------------------------------
    # MODULE A: SUGGESTIONS DROPDOWN & SEARCH CONSOLE
    # -------------------------------------------------------------
    st.header("🗂️ Global Context Search Engine")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # 🧠 FIX: Changed .tolist() to .to_list() to match Polars specifications
        suggestion_list = ["Show All Pages"] + sorted(df["page_title"].unique().to_list())
        selected_page = st.selectbox(
            "Search & Autocomplete Page Titles (Type to Filter):", 
            suggestion_list,
            index=0
        )
    with col2:
        bot_filter = st.selectbox("Filter Account Types:", ["All Accounts", "Bots Only", "Human Editors Only"])
    with col3:
        experience_filter = st.slider("Minimum Editor Experience (Prior Edits):", 0, 10000, 0)

    # Secondary text filter for comment logs
    comment_keyword = st.text_input("Optional: Filter by Edit Comment Keywords:", "")

    # Apply Polars Filtering Logic on the fly
    filtered_df = df
    
    if selected_page != "Show All Pages":
        filtered_df = filtered_df.filter(pl.col("page_title") == selected_page)
        
    if comment_keyword:
        filtered_df = filtered_df.filter(pl.col("edit_comment").str.contains(f"(?i){comment_keyword}"))
        
    if bot_filter == "Bots Only":
        filtered_df = filtered_df.filter(pl.col("is_bot") == True)
    elif bot_filter == "Human Editors Only":
        filtered_df = filtered_df.filter(pl.col("is_bot") == False)
        
    if experience_filter > 0:
        filtered_df = filtered_df.filter(pl.col("user_edit_count") >= experience_filter)

    st.subheader(f"📊 Query Search Results ({filtered_df.height} rows located)")
    st.dataframe(
        filtered_df.select([
            "page_title", "user_name", "user_edit_count", "bytes_changed", "edit_comment", "is_minor", "anomaly_score"
        ]).to_pandas(),
        width="stretch",
        hide_index=True
    )

    st.markdown("---")

    # -------------------------------------------------------------
    # MODULE B: OPERATIONAL RISK PROFILE WITH COUNT PLACEHOLDERS
    # -------------------------------------------------------------
    st.header("🤖 Operational Footprint & Volatility Risk Analytics")
    
    layout_col1, layout_col2 = st.columns(2)
    
    # Process Groupings via Polars
    bot_counts = df.group_by("is_bot").agg(pl.len().alias("Count"))
    bot_counts = bot_counts.with_columns(
        pl.col("is_bot").map_elements(lambda x: "Bot Accounts" if x else "Human Accounts", return_dtype=pl.String).alias("Account Type")
    ).to_pandas()

    bot_bytes = df.group_by("is_bot").agg(pl.col("bytes_changed").sum().alias("Total Bytes Changed"))
    bot_bytes = bot_bytes.with_columns(
        pl.col("is_bot").map_elements(lambda x: "Bot Accounts" if x else "Human Accounts", return_dtype=pl.String).alias("Account Type")
    ).to_pandas()
    
    with layout_col1:
        st.subheader("Account Allocation Counts")
        base_chart_1 = alt.Chart(bot_counts).encode(
            x=alt.X("Account Type:N", title="Account Type", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Count:Q", title="Total Tally Count")
        )
        bars_1 = base_chart_1.mark_bar(color="#1f77b4")
        text_labels_1 = base_chart_1.mark_text(align="center", baseline="bottom", dy=-5, fontSize=12, fontWeight="bold").encode(
            text="Count:Q"
        )
        st.altair_chart(bars_1 + text_labels_1, width="stretch")
        
    with layout_col2:
        st.subheader("Impact Variance: Total Volume Altered (Bytes)")
        base_chart_2 = alt.Chart(bot_bytes).encode(
            x=alt.X("Account Type:N", title="Account Type", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("Total Bytes Changed:Q", title="Total Impact Volume (Bytes)")
        )
        bars_2 = base_chart_2.mark_bar(color="#ff7f0e")
        text_labels_2 = base_chart_2.mark_text(align="center", baseline="bottom", dy=-5, fontSize=12, fontWeight="bold").encode(
            text="Total Bytes Changed:Q"
        )
        st.altair_chart(bars_2 + text_labels_2, width="stretch")

    # -------------------------------------------------------------
    # MODULE C: ACCOUNT AGE & SYSTEM EXPERIENCE RISK CORRELATION
    # -------------------------------------------------------------
    st.markdown("---")
    st.header("📉 Editor Experience vs. AI Anomaly Risk Analysis")
    st.markdown("Evaluating the strategic hypothesis: *Are newer/less experienced accounts more likely to trigger high anomaly volatility tags?*")
    
    scatter_df = df.select(["user_edit_count", "anomaly_score", "bytes_changed"])
    
    st.scatter_chart(
        data=scatter_df.to_pandas(),
        x="user_edit_count",
        y="anomaly_score",
        color="bytes_changed",
        width="stretch"
    )
