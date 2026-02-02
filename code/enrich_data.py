#!/usr/bin/env python3
"""
Enrich IPES company data using OpenAI GPT-4 - RELATIONAL SCHEMA VERSION

For each company in companies_with_filings.json:
1. Aggregates context (dockets, contacts) from nested filings
2. Queries OpenAI for market intelligence
3. Populates the 'enrichment' field
4. Outputs enriched JSON and CSV
"""

import csv
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

# OpenAI Configuration
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-mini"

# Input/Output files
INPUT_FILE = "data/structured/companies_with_filings.json"
OUTPUT_JSON = "data/enriched/companies_enriched.json"
OUTPUT_CSV = "data/enriched/companies_enriched.csv"
CACHE_FILE = "enrichment_cache.json"


def load_cache() -> Dict[str, Dict]:
    """Load cached enrichment results."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: Dict[str, Dict]):
    """Save enrichment cache."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def enrich_company(company_name: str, dockets: List[str], contacts: List[str]) -> Optional[Dict[str, Any]]:
    """
    Use OpenAI to research and enrich a company.
    """
    docket_info = ", ".join(dockets[:3]) if dockets else "Unknown"
    contact_info = ", ".join(contacts[:3]) if contacts else "Not specified"
    
    prompt = f"""Research the following company that filed for VoIP/IPES numbering authorization with the FCC:

Company Name: {company_name}
FCC Docket(s): {docket_info}
Contact/Attorney: {contact_info}

Based on your knowledge, provide the following information in JSON format:

1. is_active (boolean): Is this company still operating? True if the company appears to still be in business, False if defunct/acquired/closed.

2. activity_signal (string): Brief evidence for your is_active determination. Example: "Website active, recent press releases" or "No web presence found, domain expired"

3. industry_segment (string): Categorize as one of: "UCaaS", "CCaaS", "CPaaS", "Carrier", "Reseller", "Enterprise IT", "Healthcare", "Financial Services", "Government", "Consulting/Legal", "Unknown"

4. product_summary (string): 1-2 sentence description of what they offer. If unknown, describe based on the filing type.

5. market_position (string): Estimated scale as one of: "Enterprise", "Mid-Market", "SMB", "Startup", "Unknown"

Respond ONLY with valid JSON in this exact format, no other text:
{{"is_active": true, "activity_signal": "...", "industry_segment": "...", "product_summary": "...", "market_position": "..."}}"""

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        
        payload = {
            "model": MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a telecom industry analyst. Respond only with valid JSON, no markdown or other formatting."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 300,
            "temperature": 0.3,
        }
        
        response = requests.post(
            OPENAI_API_URL,
            headers=headers,
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        
        # Parse JSON from response
        content = content.replace("```json", "").replace("```", "").strip()
        
        enrichment = json.loads(content)
        
        # Validate expected fields
        required = ["is_active", "activity_signal", "industry_segment", "product_summary", "market_position"]
        if all(k in enrichment for k in required):
            return enrichment
        else:
            print(f"  [warning] Missing fields in response")
            return None
            
    except Exception as e:
        print(f"  [error] Enrichment failed: {e}")
        return None


def get_company_context(company: Dict) -> Tuple[List[str], List[str]]:
    """Extract unique dockets and contacts from nested filings."""
    dockets = set()
    contacts = set()
    
    for filing in company.get("filings", []):
        d = filing.get("docket_number")
        if d: dockets.add(d)

        pass
        
    return sorted(list(dockets)), sorted(list(contacts))


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    print(f"[info] Loading {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        companies = json.load(f)
    
    # Filter out bureaus
    companies = [c for c in companies if "wireline competition bureau" not in c.get("entity_name", "").lower()]
    print(f"[info] Found {len(companies)} companies to enrich")
    
    cache = load_cache()
    print(f"[info] Cached enrichments: {len(cache)}")
    
    enriched_count = 0
    cached_count = 0
    failed_count = 0
    
    for i, company in enumerate(companies):
        name = company.get("entity_name", "")
        normalized = company.get("normalized_name", "")
        
        # 1. Extract context
        dockets = set()
        for f in company.get("filings", []):
            if f.get("docket_number"):
                dockets.add(f.get("docket_number"))
        
        # 2. Check cache
        if normalized in cache:
            company["enrichment"] = cache[normalized]
            cached_count += 1
            enriched_count += 1
            continue
            
        print(f"\n[{i+1}/{len(companies)}] Enriching: {name}")
        
        # 3. Call API
        result = enrich_company(
            company_name=name,
            dockets=list(dockets),
            contacts=[] # Contacts not available in current schema, proceeding without
        )
        
        if result:
            company["enrichment"] = result
            cache[normalized] = result
            enriched_count += 1
            print(f"  [ok] {result.get('industry_segment')} - {result.get('market_position')}")
        else:
            company["enrichment"] = {}
            failed_count += 1
        
        # Save cache periodically
        if (i + 1) % 10 == 0:
            save_cache(cache)
        
        # Rate limiting
        time.sleep(21)
    
    save_cache(cache)
    
    # Write Output JSON
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)
    print(f"\n[done] Wrote {OUTPUT_JSON}")
    
    # Write Output CSV (Flattened for spreadsheet)
    csv_rows = []
    for c in companies:
        enrich = c.get("enrichment", {})
        row = {
            "id": c.get("id"),
            "entity_name": c.get("entity_name"),
            "filing_count": c.get("filing_count"),
            "is_active": enrich.get("is_active"),
            "industry_segment": enrich.get("industry_segment"),
            "market_position": enrich.get("market_position"),
            "product_summary": enrich.get("product_summary"),
            "activity_signal": enrich.get("activity_signal")
        }
        csv_rows.append(row)
        
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"[done] Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
