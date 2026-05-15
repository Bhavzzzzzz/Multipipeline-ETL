# src/ui/app.py
import streamlit as st
import subprocess
import sys
import os
import pandas as pd

# Add the controllers folder to the path so we can import the database client
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'controllers')))
import db_client

st.set_page_config(page_title="NASA Log Analytics", layout="wide")

st.title("🚀 Multi-Pipeline ETL Framework")
st.markdown("NASA HTTP Web Server Log Analytics Dashboard")

# ==========================================
# SIDEBAR: CONTROLS & EXECUTION
# ==========================================
st.sidebar.header("⚙️ Execution Controls")

pipeline_choice = st.sidebar.selectbox(
    "Select Pipeline Engine",
    ["pig", "mapreduce", "hive", "mongodb"],
    format_func=lambda x: x.capitalize() if x != "mapreduce" else "MapReduce"
)

query_choice = st.sidebar.selectbox(
    "Select Query",
    ["query1", "query2", "query3", "all"],
    format_func=lambda x: "All Queries" if x == "all" else f"Query {x[-1]}"
)

batch_size = st.sidebar.number_input("Batch Size", min_value=10000, value=100000, step=50000)

if st.sidebar.button("▶️ Run Pipeline", use_container_width=True):
    jul_path = "data/raw/NASA_access_log_Jul95.gz"
    aug_path = "data/raw/NASA_access_log_Aug95.gz"
    
    if not os.path.exists(jul_path) or not os.path.exists(aug_path):
        st.sidebar.error("Raw log files missing in data/raw/")
    else:
        with st.spinner(f"Executing {pipeline_choice.upper()} pipeline... Please wait."):
            cmd = [
                sys.executable, "src/controllers/main.py",
                "--pipeline", pipeline_choice,
                "--query", query_choice,
                "--batch-size", str(batch_size),
                "--inputs", jul_path, aug_path
            ]
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                st.sidebar.success("✅ Execution Complete!")
            except subprocess.CalledProcessError as e:
                st.sidebar.error("❌ Pipeline Failed! Check terminal for logs.")
                st.error(e.stderr)

st.sidebar.divider()
if st.sidebar.button("🗑️ Reset Database", use_container_width=True):
    db_client.reset_database()
    st.sidebar.success("Database Reset!")

# ==========================================
# MAIN AREA: REPORTING DASHBOARD
# ==========================================
st.header("📊 Latest Execution Report")

try:
    latest_run_id = db_client.get_latest_run_id()
except Exception:
    latest_run_id = None
    st.warning("Database not connected or schema not initialized.")

if not latest_run_id:
    st.info("No execution history found. Run a pipeline from the sidebar.")
else:
    meta = db_client.get_run_metadata(latest_run_id)
    
    # 1. Metadata Metrics
    st.subheader(f"Run ID: {latest_run_id} | Engine: {meta['pipeline_name']}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Runtime", f"{meta['runtime_seconds']} sec")
    col2.metric("Records Processed", f"{meta['records_processed']:,}")
    col3.metric("Malformed Records", f"{meta['malformed_record_count']:,}")
    col4.metric("Avg Batch Size", f"{meta['average_batch_size']:,.0f}")
    
    st.divider()

    # 2. Query Results Rendering
    q_filter = meta.get('query_name', 'all')
    
    if q_filter in ['query1', 'all']:
        st.subheader("Query 1: Daily Traffic Summary")
        rows_q1 = db_client.get_query_results("daily_traffic", latest_run_id, limit=100)
        if rows_q1:
            st.dataframe(pd.DataFrame(rows_q1), use_container_width=True, hide_index=True)
        else:
            st.write("No data generated.")

    if q_filter in ['query2', 'all']:
        st.subheader("Query 2: Top Requested Resources (Top 20)")
        rows_q2 = db_client.get_query_results("top_resources", latest_run_id, limit=20)
        if rows_q2:
            st.dataframe(pd.DataFrame(rows_q2), use_container_width=True, hide_index=True)
        else:
            st.write("No data generated.")

    if q_filter in ['query3', 'all']:
        st.subheader("Query 3: Hourly Error Analysis")
        rows_q3 = db_client.get_query_results("hourly_errors", latest_run_id, limit=100)
        if rows_q3:
            # Format the error_rate as a percentage for the UI
            df_q3 = pd.DataFrame(rows_q3)
            df_q3['error_rate'] = df_q3['error_rate'].astype(float).map("{:.2%}".format)
            st.dataframe(df_q3, use_container_width=True, hide_index=True)
        else:
            st.write("No data generated.")