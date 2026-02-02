#!/usr/bin/env python3
"""
Structure and deduplicate IPES filings data - RELATIONAL SCHEMA VERSION

This script transforms raw FCC filings into a normalized relational schema:
1. PARENT: Companies (deduplicated, with unique UUIDs)
2. CHILDREN: Filings (linked to parent via company_id)

Outputs:
- data/structured/companies.csv (Parent table)
- data/structured/filings.csv (Child table)
- data/structured/companies_with_filings.json (Nested JSON)
"""

import csv
import json
import re
import uuid
import os
import time
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from pathlib import Path

# Add local directory to path to ensure we can import 'schemas'
# irrespective of how the script is invoked (module vs script)
import sys
from pathlib import Path
current_dir = str(Path(__file__).parent.absolute())
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from schemas import Company
except ImportError:
    # If standard import fails, try import from current dir explicitly
    # (This handles edge cases in some environments)
    import importlib.util
    spec = importlib.util.spec_from_file_location("schemas", f"{current_dir}/schemas.py")
    schemas = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(schemas)
    Company = schemas.Company


INPUT_FILE = "data/raw/ipes_filings.json"
OUTPUT_DIR = Path("data/structured")

# Output files
OUTPUT_JSON = OUTPUT_DIR / "companies_with_filings.json"
OUTPUT_COMPANIES_CSV = OUTPUT_DIR / "companies.csv"
OUTPUT_FILINGS_CSV = OUTPUT_DIR / "filings.csv"


# Entities to exclude (not actual IPES companies)
EXCLUDE_PATTERNS = [
    r"wireline competition bureau",
    r"^fcc\b",
    r"federal communications commission",
    r"national telecommunications and information",
    r"department of justice",
    r"national association of regulatory",
]


def normalize_company_name(name: str) -> str:
    """Normalize company name for deduplication."""
    if not name:
        return ""
    
    name = name.lower().strip()
    
    # Remove common business suffixes
    suffixes = [
        r'\b(llc|l\.l\.c\.?|inc\.?|incorporated|corp\.?|corporation|co\.?|company)\b',
        r'\b(ltd\.?|limited|lp|l\.p\.?|llp|l\.l\.p\.?)\b',
        r'\b(pllc|p\.l\.l\.c\.?|pc|p\.c\.?)\b',
        r',?\s*(d/?b/?a|doing business as)\s+.*$',
    ]
    
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


def should_exclude(name: str) -> bool:
    """Check if this entity should be excluded (not an IPES company)."""
    name_lower = name.lower()
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    return False


def is_application_type(filing: Dict[str, Any]) -> bool:
    """Check if this is an application-type filing."""
    sub_type = filing.get("submission_type", "").upper()
    return any(t in sub_type for t in ["APPLICATION", "REQUEST", "PETITION"])


def is_likely_individual(name: str) -> bool:
    """Heuristic: check if name looks like an individual vs company."""
    name_lower = name.lower()
    business_indicators = ["llc", "inc", "corp", "company", "co.", "communications", 
                          "telecom", "voip", "network", "services", "solutions"]
    
    has_business_word = any(ind in name_lower for ind in business_indicators)
    parts = name.split()
    if len(parts) <= 3 and not has_business_word:
        return True
    return False


def generate_company_id(normalized_name: str) -> str:
    """Generate a deterministic UUID based on normalized name."""
    # Use a fixed namespace for consistency
    NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
    return str(uuid.uuid5(NAMESPACE, normalized_name))


def structure_data(filings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Structure filings into a relational object model.
    """
    # 1. Group filings by normalized company name
    grouped = defaultdict(list)
    
    # Filter for IPES filings only
    ipes_count = 0
    for f in filings:
        proc_desc = f.get("proceeding_description", "").lower()
        docket = f.get("docket_number", "").lower()
        
        if "voip" in proc_desc or "52.15" in proc_desc or "inbox-52.15" in docket:
            ipes_count += 1
            name = f.get("company_name", "").strip()
            if not name or should_exclude(name):
                continue
            
            normalized = normalize_company_name(name)
            if normalized:
                grouped[normalized].append(f)
    
    print(f"[info] Filtered to {ipes_count} IPES filings")
    print(f"[info] Initially grouped into {len(grouped)} unique entities")
    
    # Validation Counters
    validation_errors = 0
    error_log = []
    
    # --- DEDUPLICATION PASS ---
    # Merge highly similar groups (e.g., "Stratus Network" vs "Stratus Networks")
    import difflib
    
    # Convert dict keys to list for indexing
    keys = list(grouped.keys())
    merged_map = {} # old_key -> new_key
    
    # Sort keys by length (descending) so we merge shorter into longer/more complete names often
    # or ascending to keep base? Let's just iterate.
    keys.sort() 
    
    skip_keys = set()
    
    for i, k1 in enumerate(keys):
        if k1 in skip_keys:
            continue
            
        for k2 in keys[i+1:]:
            if k2 in skip_keys:
                continue
                
            # Check Similarity
            ratio = difflib.SequenceMatcher(None, k1, k2).ratio()
            
            # Key Logic:
            # 1. High fuzzy match (> 0.95)
            # 2. Token set match (if one is just plural of other)
            
            is_duplicate = False
            
            if ratio > 0.95:
                is_duplicate = True
            else:
                # Check for "Network" vs "Networks" type singular/plural single word diff
                # Split into words
                w1 = k1.split()
                w2 = k2.split()
                if len(w1) == len(w2):
                    diff_count = 0
                    for wa, wb in zip(w1, w2):
                        if wa != wb:
                            if wa + 's' == wb or wb + 's' == wa:
                                diff_count += 0.1 # Small penalty
                            else:
                                diff_count += 1 # Full word diff
                    
                    if diff_count < 0.2: # Only singular/plural diffs found
                        is_duplicate = True

            if is_duplicate:
                # Merge k2 into k1
                # print(f"Merging '{k2}' -> '{k1}'")
                grouped[k1].extend(grouped[k2])
                del grouped[k2]
                skip_keys.add(k2)
    
    print(f"[info] Post-deduplication: {len(grouped)} unique entities")

    structured_companies = []
    
    for normalized_name, company_filings in grouped.items():
        # Determine primary name (longest variant usually best)
        all_names = [f.get("company_name", "") for f in company_filings]
        primary_name = max(all_names, key=len) if all_names else normalized_name
        
        # Check heuristics
        is_individual = is_likely_individual(primary_name)
        has_application = any(is_application_type(f) for f in company_filings)
        
        # Process filings (Child records)
        processed_filings = []
        for f in company_filings:
            filing_record = {
                "filing_id": f.get("submission_id"),
                "date_received": f.get("date_received"),
                "docket_number": f.get("docket_number"),
                "submission_type": f.get("submission_type"),
                "filing_status": f.get("filing_status"),
                "document_urls": f.get("document_urls", "").split("; ") if f.get("document_urls") else [],
                "detail_url": f.get("detail_url")
            }
            # Clean up empty URL lists
            if not filing_record["document_urls"] or filing_record["document_urls"] == ['']:
                filing_record["document_urls"] = []
                
            processed_filings.append(filing_record)
        
        # Sort filings by date desc
        processed_filings.sort(key=lambda x: x.get("date_received", ""), reverse=True)
        
        # Create Parent record
        company_id = generate_company_id(normalized_name)
        
        company_record = {
            "id": company_id,
            "entity_name": primary_name,
            "normalized_name": normalized_name,
            "entity_type": "Individual" if is_individual else "Company",
            "is_applicant": has_application,
            "filing_count": len(processed_filings),
            "enrichment": {},  # Placeholder for Part 3
            "filings": processed_filings
        }
        

        # Only include actual applicants (companies with applications)
        if has_application and not is_individual:
            try:
                # INTEGRITY CHECK: Validate with Pydantic
                validated_company = Company(**company_record)
                # Convert back to dict for JSON serialization
                structured_companies.append(validated_company.model_dump())
            except Exception as e:
                print(f"[warning] Validation failed for {normalized_name}: {e}")
                validation_errors += 1
                error_log.append({"name": normalized_name, "error": str(e)})

            
    # Sort companies by latest filing date
    structured_companies.sort(
        key=lambda x: x.get("latest_filing_date", ""), 
        reverse=True
    )
    
    print(f"[info] Generated {len(structured_companies)} structured company records")
    
    validation_stats = {
        "timestamp": datetime.now().isoformat(),
        "total_processed": len(structured_companies) + validation_errors, # approximate totals
        "valid_records": len(structured_companies),
        "invalid_records": validation_errors,
        "error_samples": error_log[:5] 
    }
    
    # Append to history
    val_history = []
    
    # Ensure directory exists
    json_dir = Path("data/monitoring")
    json_dir.mkdir(parents=True, exist_ok=True)
    val_file = json_dir / "validation_stats.json"
    
    if val_file.exists():
        try:
            with open(val_file, "r") as f:
                content = json.load(f)
                val_history = content if isinstance(content, list) else [content]
        except Exception:
            val_history = []
            
    val_history.append(validation_stats)
    
    with open(val_file, "w") as f:
        json.dump(val_history, f, indent=2)

    return structured_companies


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    print(f"[info] Loading {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Run structuring
    global validation_errors, error_log
    validation_errors = 0
    error_log = []
    
    structured_data = structure_data(data)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Output Nested JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(structured_data, f, indent=2, ensure_ascii=False)
    print(f"[done] Wrote {OUTPUT_JSON}")
    
    # 2. Output CSVs (Relational)
    
    # Parent Table: companies.csv
    companies_rows = []
    filings_rows = []
    
    for company in structured_data:
        # Parent Row
        companies_rows.append({
            "id": company["id"],
            "entity_name": company["entity_name"],
            "normalized_name": company["normalized_name"],
            "entity_type": company["entity_type"],
            "filing_count": company["filing_count"],
            "latest_filing_date": company["filings"][0]["date_received"] if company["filings"] else ""
        })
        
        # Child Rows
        for filing in company["filings"]:
            filings_rows.append({
                "company_id": company["id"],  # Foreign Key
                "filing_id": filing["filing_id"],
                "date_received": filing["date_received"],
                "docket_number": filing["docket_number"],
                "submission_type": filing["submission_type"],
                "status": filing["filing_status"],
                "primary_doc_url": filing["document_urls"][0] if filing["document_urls"] else ""
            })
            
    # Write companies.csv
    with open(OUTPUT_COMPANIES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "entity_name", "normalized_name", "entity_type", "filing_count", "latest_filing_date"])
        writer.writeheader()
        writer.writerows(companies_rows)
    print(f"[done] Wrote {OUTPUT_COMPANIES_CSV}")
    
    # Write filings.csv
    with open(OUTPUT_FILINGS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["company_id", "filing_id", "date_received", "docket_number", "submission_type", "status", "primary_doc_url"])
        writer.writeheader()
        writer.writerows(filings_rows)
    print(f"[done] Wrote {OUTPUT_FILINGS_CSV}")
    
    # Sample verification
    print("\n[sample] JSON Structure (First Record):")
    print(json.dumps(structured_data[0], indent=2)[:300] + "...")


if __name__ == "__main__":
    main()