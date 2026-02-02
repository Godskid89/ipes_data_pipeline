import streamlit as st

st.set_page_config(
    page_title="IPES Data Pipeline",
    page_icon="📡",
    layout="wide"
)

st.title("📡 IPES Data Pipeline")

st.markdown("""
### Welcome
This dashboard provides a visual interface for the **IPES Data Pipeline**.

#### 📂 Modules
*   **🚀 Control Center**: Manually trigger the pipeline or configure the automated scheduler.
*   **📊 Monitoring**: View health checks, execution logs, and data integrity reports.
*   **📈 Market Insights**: Interactive visualizations of the enriched telecom data.

---
**Status**: The pipeline is ready. Use the sidebar to navigate.
""")

st.sidebar.success("Select a page above.")
