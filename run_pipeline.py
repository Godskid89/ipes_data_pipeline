#!/usr/bin/env python3
"""
Master Pipeline Script for Sent Growth Engineer Assessment.

This script orchestrates the entire data pipeline:
1. FETCH: Extracts fresh filings from FCC ECFS API -> data/raw/ipes_filings.json
2. STRUCTURE: Transforms flat filings into relational schema -> data/structured/
3. ENRICH: Enriches companies with OpenAI -> data/enriched/
4. DOWNLOAD: Downloads sample PDF documents -> documents/

Usage:
    python3 run_pipeline.py [--skip-fetch] [--skip-download]
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
import time
import json

# Configuration
PROJECT_ROOT = Path(__file__).parent.absolute()
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_STRUCTURED = PROJECT_ROOT / "data" / "structured"
DATA_ENRICHED = PROJECT_ROOT / "data" / "enriched"
DOCS_DIR = PROJECT_ROOT / "documents"

def run_step(step_name, command):
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print(f"CMD:  {command}")
    print(f"{'='*60}")
    
    start_time = time.time()
    result = subprocess.run(command, shell=True)
    duration = time.time() - start_time
    
    if result.returncode != 0:
        print(f"\n[ERROR] Step '{step_name}' failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    
    print(f"\n[SUCCESS] Step '{step_name}' completed in {duration:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Run IPES Market Intelligence Pipeline")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip fetching fresh data from FCC")
    parser.add_argument("--skip-download", action="store_true", help="Skip PDF document downloads")
    parser.add_argument("--doc-limit", type=int, default=0, help="Number of PDFs to download (0 = all, default: 0)")
    args = parser.parse_args()

    # Ensure directories exist
    for d in [DATA_RAW, DATA_STRUCTURED, DATA_ENRICHED, DOCS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # 1. FETCH DATA
    if not args.skip_fetch:
        # fetch.py outputs based on --out-prefix. We want it in data/raw/ipes_filings
        out_prefix = DATA_RAW / "ipes_filings"
        # Fetch ALL records (max-records 0 = everything)
        cmd = f"python3 code/fetch.py --limit 100 --max-records 0 --out-prefix '{out_prefix}'"
        run_step("Fetch ALL Data from FCC", cmd)
    else:
        print("\n[SKIP] Step 'Fetch Data' skipped")

    # 2. STRUCTURE DATA (Relational Schema)
    # structure_data.py reads data/raw/ipes_filings.json and outputs to data/structured/
    cmd = "python3 code/structure_data.py"
    run_step("Structure Data (Relational)", cmd)

    # 3. ENRICH DATA
    # enrich_data.py reads data/structured/companies_with_filings.json and outputs to data/enriched/
    cmd = "python3 code/enrich_data.py"
    run_step("Enrich Data (OpenAI)", cmd)

    # 4. DOWNLOAD DOCUMENTS
    if not args.skip_download:
        if args.doc_limit > 0:
            cmd = f"python3 code/download_pdfs_robust.py --limit {args.doc_limit}"
            step_name = f"Download PDFs (Limit: {args.doc_limit})"
        else:
            cmd = "python3 code/download_pdfs_robust.py --all"
            step_name = "Download PDFs (All)"
        
        run_step(step_name, cmd)
    else:
        print("\n[SKIP] Step 'Download PDFs' skipped")
    
    # END OF PIPELINE

    total_duration = time.time() - pipeline_start
    
    # Load Validation Stats if available
    val_stats = {}
    val_path = Path("data/monitoring/validation_stats.json")
    if val_path.exists():
        with open(val_path, "r") as f:
            content = json.load(f)
            # If it's a history list, grab the latest one
            if isinstance(content, list) and content:
                val_stats = content[-1]
            else:
                val_stats = content
    
    stats = {
        "status": "success",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_duration_seconds": round(total_duration, 2),
        "validation_report": val_stats, # Added validation report
        "steps": step_metrics,
        "outputs": {
            "raw_filings": str(DATA_RAW / "ipes_filings.json"),
            "structured_companies": str(DATA_STRUCTURED / "companies.csv"),
            "enriched_data": str(DATA_ENRICHED / "companies_enriched.json"),
            "documents_folder": str(DOCS_DIR)
        }
    }
    
    # Append to history
    history = []
    
    # Ensure directory exists
    json_dir = Path("data/monitoring")
    json_dir.mkdir(parents=True, exist_ok=True)
    
    stats_file = json_dir / "run_stats.json"
    
    if stats_file.exists():
        try:
            with open(stats_file, "r") as f:
                content = json.load(f)
                if isinstance(content, list):
                    history = content
                else:
                    history = [content] # Convert old format
        except Exception:
            history = []
    
    history.append(stats)
    
    with open(stats_file, "w") as f:
        json.dump(history, f, indent=2)

    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE! 🚀")
    print(f"{'='*60}")
    print(f"Monitoring: Run stats saved to {stats_file}")
    print(f"Outputs:")
    print(f"  - Raw:        {DATA_RAW}/ipes_filings.json")

if __name__ == "__main__":
    step_metrics = {}
    pipeline_start = time.time()
    try:
        main()
    except Exception as e:
        # Last mile catch to write failed stats
        with open("data/run_stats.json", "w") as f:
            json.dump({
                "status": "failed", 
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }, f, indent=2)
        raise
