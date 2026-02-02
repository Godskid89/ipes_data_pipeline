import streamlit as st
import pandas as pd
import json
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Monitoring", page_icon="📊", layout="wide")
st.title("📊 Pipeline Monitoring")

# Paths - Robust resolution relative to this script file
# Script is in dashboard/pages/2_...py
# Root is ../../
ROOT_DIR = Path(__file__).parents[2]
MONITORING_DIR = ROOT_DIR / "data/monitoring"

RUN_STATS_FILE = MONITORING_DIR / "run_stats.json"
VAL_STATS_FILE = MONITORING_DIR / "validation_stats.json"

col1, col2 = st.columns(2)

# --- RUN STATS ---
with col1:
    st.subheader("🚀 Execution History")
    if RUN_STATS_FILE.exists():
        try:
            with open(RUN_STATS_FILE, "r") as f:
                data = json.load(f)
                # Handle old single-object format vs new list format
                if isinstance(data, dict):
                    data = [data]
                
                if data:
                    df_runs = pd.DataFrame(data)
                    
                    # Key Metrics
                    latest = df_runs.iloc[-1]
                    status_color = "green" if latest.get("status") == "success" else "red"
                    st.markdown(f"**Last Run Status**: :{status_color}[{latest.get('status', 'Unknown').upper()}]")
                    st.markdown(f"**Last Runtime**: {latest.get('total_duration_seconds', 0)} seconds")
                    st.markdown(f"**Timestamp**: {latest.get('timestamp', 'Unknown')}")
                    
                    # History Table
                    st.dataframe(
                        df_runs[["timestamp", "status", "total_duration_seconds"]].sort_values("timestamp", ascending=False),
                        use_container_width=True
                    )
                else:
                    st.warning("Run stats file is empty.")
        except Exception as e:
            st.error(f"Error loading run stats: {e}")
    else:
        st.warning(f"No run stats found at {RUN_STATS_FILE}")

# --- VALIDATION STATS ---
with col2:
    st.subheader("🛡️ Data Integrity")
    if VAL_STATS_FILE.exists():
        try:
            with open(VAL_STATS_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data = [data]
                
                if data:
                    df_val = pd.DataFrame(data)
                    latest_val = df_val.iloc[-1]
                    
                    total = latest_val.get("total_processed", 0)
                    valid = latest_val.get("valid_records", 0)
                    invalid = latest_val.get("invalid_records", 0)
                    
                    # Metric Cards
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Total Records", total)
                    m2.metric("Valid Logs", valid)
                    m3.metric("Invalid Logs", invalid, delta_color="inverse")
                    
                    # Pie Chart
                    fig = px.pie(
                        names=["Valid", "Invalid"], 
                        values=[valid, invalid],
                        title="Data Quality Distribution (Latest Run)",
                        color_discrete_sequence=["#00CC96", "#EF553B"]
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Error Logs
                    st.subheader("Top Validation Errors")
                    errors = latest_val.get("error_samples", [])
                    if errors:
                        st.json(errors)
                    else:
                        st.success("No validation errors in last run! 🎉")
                        
                else:
                    st.warning("Validation stats file is empty.")
        except Exception as e:
            st.error(f"Error loading validation stats: {e}")
    else:
        st.warning(f"No validation stats found at {VAL_STATS_FILE}")
