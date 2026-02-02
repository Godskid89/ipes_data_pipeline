import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Data Insights", page_icon="📈", layout="wide")
st.title("📈 Insights & Analytics")

# Paths - Robust resolution
ROOT_DIR = Path(__file__).parents[2]
COMPANIES_CSV = ROOT_DIR / "data/structured/companies.csv"
FILINGS_CSV = ROOT_DIR / "data/structured/filings.csv"

@st.cache_data
def load_and_join_data():
    if not COMPANIES_CSV.exists() or not FILINGS_CSV.exists():
        return pd.DataFrame()
    
    try:
        # Load CSVs
        df_co = pd.read_csv(COMPANIES_CSV)
        df_filings = pd.read_csv(FILINGS_CSV)
        
        # Merge: Filings is the fact table, Companies is dimension
        df_merged = pd.merge(
            df_filings, 
            df_co, 
            left_on="company_id", 
            right_on="id", 
            how="inner",
            suffixes=("_filing", "_company")
        )
        
        # Date Conversion
        if "date_received" in df_merged.columns:
            df_merged["date_received"] = pd.to_datetime(df_merged["date_received"], errors="coerce")
            
        return df_merged
        
    except Exception as e:
        st.error(f"Error merging data: {e}")
        return pd.DataFrame()

df = load_and_join_data()

if not df.empty:
    # --- METRICS ROW ---
    st.subheader("High Level Overview")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Filings", len(df))
    m2.metric("Unique Companies", df["entity_name"].nunique())
    m3.metric("Top Docket", df["docket_number"].mode()[0] if not df["docket_number"].empty else "N/A")
    m4.metric("Latest Activity", df["date_received"].max().strftime('%Y-%m-%d') if not df["date_received"].isnull().all() else "N/A")

    st.markdown("---")

    # --- ROW 1: TIME SERIES ---
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📅 Filing Activity Over Time")
        # Aggregation by Month
        if "date_received" in df.columns:
            df["month_year"] = df["date_received"].dt.to_period("M").dt.to_timestamp()
            time_counts = df.groupby("month_year").size().reset_index(name="count")
            
            fig_time = px.line(time_counts, x="month_year", y="count", markers=True, title="Filings Trend (Monthly)")
            fig_time.update_layout(xaxis_title="Month", yaxis_title="Number of Filings")
            st.plotly_chart(fig_time, use_container_width=True)
            
    with col2:
        st.subheader("📂 Docket Distribution")
        docket_counts = df["docket_number"].value_counts().head(5).reset_index()
        docket_counts.columns = ["Docket", "Count"]
        
        fig_docket = px.pie(docket_counts, names="Docket", values="Count", hole=0.4, title="Top 5 Active Dockets")
        st.plotly_chart(fig_docket, use_container_width=True)

    # --- ROW 2: TOP FILERS & STATUS ---
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("🏢 Top 10 Most Active Companies")
        top_filers = df["entity_name"].value_counts().head(10).reset_index()
        top_filers.columns = ["Company", "Filings"]
        
        fig_top = px.bar(
            top_filers, 
            x="Filings", 
            y="Company", 
            orientation="h", 
            color="Filings",
            title="Companies by Filing Volume",
            color_continuous_scale="Viridis"
        )
        fig_top.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_top, use_container_width=True)

    with col4:
        st.subheader("📊 Filing Types")
        if "submission_type" in df.columns:
            type_counts = df["submission_type"].value_counts().head(7).reset_index()
            type_counts.columns = ["Type", "Count"]
            
            fig_type = px.bar(type_counts, x="Type", y="Count", title="Submission Types")
            st.plotly_chart(fig_type, use_container_width=True)
        else:
            st.info("Submission Type not available.")

    # --- RAW DATA EXPANDER ---
    with st.expander("🔎 View Raw Merged Data"):
        st.dataframe(df.sort_values("date_received", ascending=False))

else:
    st.warning(f"No data links found. Ensure '{COMPANIES_CSV}' and '{FILINGS_CSV}' exist.")
