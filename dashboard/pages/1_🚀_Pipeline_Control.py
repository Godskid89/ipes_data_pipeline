import streamlit as st
import subprocess
import sys
from pathlib import Path
import json
import time
import os
from datetime import datetime

st.set_page_config(page_title="Pipeline Control", page_icon="🚀")
st.title("🚀 Pipeline Control Center")

# --- MANUAL TRIGGER ---

st.header("Manual Execution")
st.markdown("Trigger a one-off execution of the full data pipeline.")

col1, col2 = st.columns(2)
skip_fetch = col1.checkbox("Skip Fetch (Use cached)", value=True)

# PDF Downloads are terminal-only
st.info("📥 **PDF Downloads**: Run via terminal with `python3 run_pipeline.py`")

if st.button("▶️ Run Pipeline Now", type="primary"):
    # Always skip downloads in Streamlit UI
    cmd = [sys.executable, "run_pipeline.py", "--skip-download"]
    if skip_fetch:
        cmd.append("--skip-fetch")
    
    st.info(f"Executing: `{' '.join(cmd)}`")

    
    # Stream output
    output_container = st.empty()
    full_output = []
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=Path(__file__).parents[2] # Go up to root (pages -> dashboard -> root)
        )
        
        with st.status("Pipeline Running...", expanded=True) as status:
            for line in iter(process.stdout.readline, ''):
                clean_line = line.strip()
                full_output.append(clean_line)
                st.write(clean_line) # Write to expander
                
            process.wait()
            
            if process.returncode == 0:
                status.update(label="Pipeline Completed Successfully! ✅", state="complete", expanded=False)
                st.success("Run finished.")
                st.balloons()
            else:
                status.update(label="Pipeline Failed ❌", state="error", expanded=True)
                st.error("Pipeline finished with errors.")
                
    except Exception as e:
        st.error(f"Failed to start process: {e}")

st.divider()

st.divider()

# --- SCHEDULER MANAGEMENT ---
st.header("⏳ Scheduled Jobs")
st.markdown("Manage automated pipeline runs.")

# Path: dashboard/pages/../config/scheduler_jobs.json
JOBS_FILE = Path(__file__).parent.parent / "config/scheduler_jobs.json"
# Ensure config dir exists
JOBS_FILE.parent.mkdir(parents=True, exist_ok=True)

def load_jobs():
    if not JOBS_FILE.exists():
        return []
    try:
        with open(JOBS_FILE, "r") as f:
            return json.load(f)
    except:
        return []

def save_jobs(jobs):
    with open(JOBS_FILE, "w") as f:
        json.dump(jobs, f, indent=2)

# --- CREATE JOB ---
with st.expander("➕ Add New Schedule", expanded=False):
    with st.form("new_job_form"):
        col_f, col_t = st.columns(2)
        interval = col_f.selectbox("Frequency", ["Hourly", "Daily", "Weekly"])
        
        # Default time
        default_time = datetime.strptime("09:00", "%H:%M").time()
        run_time = col_t.time_input("Run Time (UTC)", value=default_time)
        
        submitted = st.form_submit_button("Add Schedule")
        
        if submitted:
            new_job = {
                "id": str(int(time.time()*1000)), # Simple unique ID
                "interval": interval,
                "time": run_time.strftime("%H:%M"),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M")
            }
            jobs = load_jobs()
            jobs.append(new_job)
            save_jobs(jobs)
            st.success(f"Added {interval} job at {new_job['time']}")
            st.rerun()

# --- INSTALLED JOBS LIST ---
current_jobs = load_jobs()

if current_jobs:
    st.subheader(f"Active Schedules ({len(current_jobs)})")
    
    # Table header
    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
    c1.markdown("**Frequency**")
    c2.markdown("**Time (UTC)**")
    c3.markdown("**Created**")
    c4.markdown("**Action**")
    
    st.divider()
    
    for job in current_jobs:
        c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
        c1.write(job.get("interval"))
        c2.write(job.get("time"))
        c3.write(job.get("created_at", "-"))
        
        if c4.button("🗑️", key=f"del_{job['id']}", help="Delete Schedule"):
            # Update list
            updated_jobs = [j for j in current_jobs if j["id"] != job["id"]]
            save_jobs(updated_jobs)
            st.rerun()
else:
    st.info("No active schedules. Add one above!")
