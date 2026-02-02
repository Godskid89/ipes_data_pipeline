#!/usr/bin/env python3
"""
Fetch IPES (Interconnected VoIP Provider) numbering authorization filings from FCC ECFS.

IPES filings are found in proceeding "INBOX-52.15" (VoIP Numbering Authorization Applications)
and related dockets containing "Interconnected VoIP Numbering Authorization".

API endpoint: https://publicapi.fcc.gov/ecfs/filings (requires api_key)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://publicapi.fcc.gov/ecfs"

# Multiple queries to capture all IPES filings
IPES_QUERIES = [
    # Primary: Direct INBOX-52.15 filings
    'proceedings.name:"INBOX-52.15"',
    # Secondary: Individual dockets with VoIP Numbering Authorization
    'proceedings.description:"Interconnected VoIP Numbering Authorization"',
    # Tertiary: Section 52.15 references
    'proceedings.description:"52.15"',
]

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

FCC_ECFS_API_KEY = os.getenv("FCC_ECFS_API_KEY")
if not FCC_ECFS_API_KEY:
    # Fallback or error warning
    print("[warning] FCC_ECFS_API_KEY not found in environment")
    FCC_ECFS_API_KEY = ""


def request_page(
    api_key: str,
    q: str,
    limit: int,
    offset: int,
    sort: str,
    timeout: int = 120,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch a single page and return (records, total).
    """
    url = f"{BASE_URL}/filings"
    params = {
        "api_key": api_key,
        "q": q,
        "limit": limit,
        "offset": offset,
        "sort": sort,
    }

    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()

    data = r.json()
    
    # API returns {"filing": [...], "aggregations": {...}}
    records = data.get("filing", [])
    
    # Total from headers or aggregation
    total = 0
    if "total" in r.headers:
        total = int(r.headers["total"])
    elif "aggregations" in data:
        # Sometimes total is in aggregations
        agg = data.get("aggregations", {})
        if "total" in agg:
            total = agg["total"]
    
    return records, total


def safe_get(d: Any, *keys: str, default: str = "") -> str:
    """Safely get nested dict values."""
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return str(cur) if cur is not None else default


def normalize_filing(filing: Dict[str, Any]) -> Dict[str, Any]:
    """Extract structured data from a filing record."""
    submission_id = safe_get(filing, "id_submission")
    
    # Dates
    date_received = safe_get(filing, "date_received")
    date_disseminated = safe_get(filing, "date_disseminated")
    
    # Submission type
    submission_type = safe_get(filing, "submissiontype", "description")
    
    # Get proceeding/docket info
    procs = filing.get("proceedings", [])
    dockets = []
    proc_descriptions = []
    for p in procs:
        if isinstance(p, dict):
            name = safe_get(p, "name")
            desc = safe_get(p, "description")
            if name:
                dockets.append(name)
            if desc:
                proc_descriptions.append(desc)
    
    # Bureau
    bureau = ""
    for p in procs:
        if isinstance(p, dict):
            b = safe_get(p, "bureau_name")
            if b:
                bureau = b
                break
    
    # Filer info
    filers = filing.get("filers", [])
    filer_names = []
    for f in filers:
        if isinstance(f, dict):
            name = safe_get(f, "name")
            if name:
                filer_names.append(name)
    
    # Authors (often the contact/attorney)
    authors = filing.get("authors", [])
    author_names = []
    for a in authors:
        if isinstance(a, dict):
            name = safe_get(a, "name")
            if name:
                author_names.append(name)
    
    # Law firms
    lawfirms = filing.get("lawfirms", [])
    lawfirm_names = []
    for lf in lawfirms:
        if isinstance(lf, dict):
            name = safe_get(lf, "name")
            if name:
                lawfirm_names.append(name)
        elif isinstance(lf, str):
            lawfirm_names.append(lf)
    
    # Documents
    docs = filing.get("documents", [])
    doc_urls = []
    for d in docs:
        if isinstance(d, dict):
            src = safe_get(d, "src")
            if src:
                doc_urls.append(src)
    
    # Filing status
    status = safe_get(filing, "filingstatus", "description")
    
    # Detail URL
    detail_url = f"https://www.fcc.gov/ecfs/filing/{submission_id}" if submission_id else ""
    
    return {
        "submission_id": submission_id,
        "company_name": "; ".join(filer_names),
        "date_received": date_received[:10] if len(date_received) > 10 else date_received,
        "submission_type": submission_type,
        "docket_number": "; ".join(dockets),
        "proceeding_description": "; ".join(proc_descriptions)[:200],
        "bureau": bureau,
        "filing_status": status,
        "contact_attorney": "; ".join(author_names),
        "law_firm": "; ".join(lawfirm_names),
        "document_urls": "; ".join(doc_urls),
        "detail_url": detail_url,
    }


def fetch_all_filings(
    api_key: str,
    query: str,
    limit: int = 100,
    max_records: Optional[int] = None,
    sleep_s: float = 0.5,
) -> List[Dict[str, Any]]:
    """Fetch all pages of results."""
    all_records: List[Dict[str, Any]] = []
    offset = 0
    total = None
    
    while True:
        print(f"[fetch] Requesting offset={offset}, limit={limit}...")
        try:
            records, resp_total = request_page(
                api_key=api_key,
                q=query,
                limit=limit,
                offset=offset,
                sort="date_received,DESC",
            )
        except requests.exceptions.HTTPError as e:
            print(f"[error] HTTP error: {e}")
            if e.response is not None:
                print(f"[error] Response: {e.response.text[:500]}")
            break
        except Exception as e:
            print(f"[error] Request failed: {e}")
            break
        
        if total is None and resp_total > 0:
            total = resp_total
            print(f"[info] Total available: {total}")
        
        all_records.extend(records)
        print(f"[fetch] Got {len(records)} records (total fetched: {len(all_records)})")
        
        # Stop conditions
        if not records:
            break
        if len(records) < limit:
            break
        if total and len(all_records) >= total:
            break
        if max_records and len(all_records) >= max_records:
            break
        
        offset += limit
        if sleep_s > 0:
            time.sleep(sleep_s)
    
    return all_records


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Fetch IPES numbering authorization applications from FCC ECFS"
    )
    ap.add_argument(
        "--api-key",
        default=os.getenv("FCC_ECFS_API_KEY", FCC_ECFS_API_KEY),
        help="API key"
    )
    ap.add_argument(
        "--query",
        default=None,
        help="Custom search query (overrides default IPES queries)"
    )
    ap.add_argument("--limit", type=int, default=100, help="Page size")
    ap.add_argument("--max-records", type=int, default=None, help="Max records per query")
    ap.add_argument("--sleep", type=float, default=0.5, help="Sleep between calls")
    ap.add_argument("--out-prefix", default="ipes_filings", help="Output file prefix")
    args = ap.parse_args()

    if not args.api_key:
        print("[error] API key required. Set FCC_ECFS_API_KEY or use --api-key")
        return 1

    print(f"[info] Using API key: yes")
    
    # Use custom query or run all default IPES queries
    queries = [args.query] if args.query else IPES_QUERIES
    
    all_filings: List[Dict[str, Any]] = []
    seen_ids = set()
    
    for query in queries:
        print(f"\n[info] Query: {query}")
        
        filings = fetch_all_filings(
            api_key=args.api_key,
            query=query,
            limit=args.limit,
            max_records=args.max_records,
            sleep_s=args.sleep,
        )
        
        # Deduplicate by submission ID
        new_count = 0
        for f in filings:
            sid = f.get("id_submission", "")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                all_filings.append(f)
                new_count += 1
        
        print(f"[info] New unique filings from this query: {new_count}")
    
    if not all_filings:
        print("\n[warning] No filings found!")
        return 1
    
    # Normalize
    normalized = [normalize_filing(f) for f in all_filings]
    # Write outputs
    out_json = f"{args.out_prefix}.json"
    out_csv = f"{args.out_prefix}.csv"
    
    with open(out_json, "w", encoding="utf-8") as fp:
        json.dump(normalized, fp, indent=2, ensure_ascii=False)
    
    csv_fields = [
        "submission_id", "company_name", "date_received", "submission_type",
        "docket_number", "proceeding_description", "bureau", "filing_status",
        "contact_attorney", "law_firm", "document_urls", "detail_url"
    ]
    with open(out_csv, "w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=csv_fields)
        w.writeheader()
        for row in normalized:
            w.writerow({k: row.get(k, "") for k in csv_fields})
    
    print(f"\n[done] Total IPES filings: {len(normalized)}")
    print(f"[done] Wrote: {out_csv}")
    print(f"[done] Wrote: {out_json}")
    
    # Show sample
    if normalized:
        print("\n[sample] First 3 companies found:")
        for r in normalized[:3]:
            print(f"  - {r['company_name']} ({r['date_received']})")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
