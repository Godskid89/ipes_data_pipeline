import schedule
import time
import json
import subprocess
import sys
from pathlib import Path
import logging

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [SCHEDULER] - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)

CONFIG_FILE = Path(__file__).parent / "config/scheduler_config.json"
JOBS_FILE = Path(__file__).parent / "config/scheduler_jobs.json"
ROOT_DIR = Path(__file__).parent.parent

def run_job(job_id):
    logging.info(f"🚀 Triggering Pipeline (Job ID: {job_id})")
    try:
        # Run the full pipeline
        cmd = [sys.executable, "run_pipeline.py", "--doc-limit", "5"]
        result = subprocess.run(
            cmd, 
            cwd=ROOT_DIR,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logging.info(f"✅ Job {job_id} Success")
        else:
            logging.error(f"❌ Job {job_id} Failed:\n{result.stderr}")
            
    except Exception as e:
        logging.error(f"💥 Exception in Job {job_id}: {e}")

def load_jobs():
    if not JOBS_FILE.exists():
        # Fallback to old config if partial migration
        return []
    try:
        with open(JOBS_FILE, "r") as f:
            return json.load(f)
    except:
        return []

def main():
    logging.info("Starting Multi-Job Scheduler Service...")
    
    last_jobs_data = None
    
    while True:
        # Hot-reload config
        current_jobs = load_jobs()
        
        # Check if changed (simple content compare)
        current_data_str = json.dumps(current_jobs, sort_keys=True)
        
        if current_data_str != last_jobs_data:
            schedule.clear()
            logging.info(f"Configuration changed. Reloading {len(current_jobs)} jobs.")
            
            for job in current_jobs:
                jid = job.get("id")
                interval = job.get("interval")
                run_time = job.get("time")
                
                try:
                    if interval == "Hourly":
                        schedule.every().hour.do(run_job, job_id=jid)
                        logging.info(f"Registered Job {jid}: Hourly")
                    elif interval == "Daily":
                        schedule.every().day.at(run_time).do(run_job, job_id=jid)
                        logging.info(f"Registered Job {jid}: Daily at {run_time}")
                    elif interval == "Weekly":
                        schedule.every().monday.at(run_time).do(run_job, job_id=jid)
                        logging.info(f"Registered Job {jid}: Weekly (Mon) at {run_time}")
                except Exception as e:
                    logging.error(f"Failed to register job {jid}: {e}")
            
            if not current_jobs:
                logging.info("No active jobs found. Scheduler idle.")

            last_jobs_data = current_data_str
            
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    main()
