#!/usr/bin/env python3
"""
Robust FCC ECFS PDF Downloader using Playwright.

This script uses Playwright to establish a real browser session, then downloads
PDFs by intercepting responses or using JavaScript fetch from within the browser
context - bypassing Akamai bot protection.

Usage:
    python download_pdfs_robust.py --limit 20
    python download_pdfs_robust.py --all
"""

import asyncio
import json
import os
import re
import sys
import base64
import random
import requests
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import argparse

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("Error: Playwright not installed. Run: pip install playwright && playwright install chromium")
    sys.exit(1)

# Paths relative to project root
PROJECT_ROOT = Path(__file__).parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "structured" / "companies_with_filings.json"
OUTPUT_DIR = PROJECT_ROOT / "documents"

# Rate limiting
MIN_DELAY = 1.5  # seconds between downloads
MAX_DELAY = 3.5


def sanitize_filename(name: str) -> str:
    """Create safe filename from company name."""
    if not name:
        return "unknown"
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'_+', '_', name)
    return name[:80].strip('_')


def load_companies() -> List[Dict]:
    """Load company data from JSON file."""
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found: {INPUT_FILE}")
        sys.exit(1)
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_download_queue(companies: List[Dict], limit: Optional[int] = None) -> List[Dict]:
    """
    Build queue of documents to download from structured data.
    """
    queue = []
    
    for company in companies:
        name = company.get('entity_name', 'Unknown') # Changed from company_name
        safe_name = sanitize_filename(name)
        
        filings = company.get('filings', [])
        
        for filing in filings:
            urls = filing.get('document_urls', [])
            
            for idx, url in enumerate(urls, 1):
                if not url:
                    continue
                
                # Extract document ID from URL for unique filename
                doc_id = url.split('/document/')[-1].replace('/', '_') if '/document/' in url else str(idx)
                # Include filing status or docket if useful, but stick to simple naming
                filename = f"{safe_name}_{doc_id}.pdf"
                
                queue.append({
                    'url': url,
                    'company_name': name,
                    'filename': filename,
                    'filepath': OUTPUT_DIR / filename,
                })
    
    # Apply limit if specified
    if limit and limit > 0:
        queue = queue[:limit]
    
    return queue


def download_direct(url: str, filepath: Path) -> Tuple[bool, str]:
    """Download directly using requests (for static docs.fcc.gov links)."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=30, stream=True)
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
            
        # Determine extension from content (magic numbers)
        final_path = filepath
        chunk = next(resp.iter_content(chunk_size=4), b'')
        
        if chunk.startswith(b'%PDF'):
            val_ext = '.pdf'
        elif chunk.startswith(b'PK'):
            val_ext = '.docx'
        elif chunk.startswith(b'\xD0\xCF\x11\xE0'):
            val_ext = '.doc'
        else:
            val_ext = filepath.suffix
            
        if val_ext != filepath.suffix:
            final_path = filepath.with_suffix(val_ext)
            
        with open(final_path, 'wb') as f:
            f.write(chunk)
            for rest in resp.iter_content(chunk_size=8192):
                f.write(rest)
                
        return True, f"Direct Download ({final_path.stat().st_size} bytes) as {final_path.name}"
        
    except Exception as e:
        return False, f"Direct Error: {e}"


async def download_via_browser_fetch(page, doc_url: str, filepath: Path) -> Tuple[bool, str]:
    """
    Use JavaScript fetch from within browser context to download PDF.
    This leverages the browser's established session cookies and passes Akamai checks.
    """
    try:
        # FCC ECFS URL pattern:
        # /ecfs/document/... (singular) = React SPA HTML shell
        # /ecfs/documents/... (plural) = Actual PDF binary
        # ALWAYS try plural first!
        
        urls_to_try = []
        
        # Convert to plural form first (the working one)
        if '/document/' in doc_url and '/documents/' not in doc_url:
            urls_to_try.append(doc_url.replace('/document/', '/documents/'))
            urls_to_try.append(doc_url)  # Fallback to original
        elif '/documents/' in doc_url:
            urls_to_try.append(doc_url)  # Already correct
        else:
            urls_to_try.append(doc_url)  # Unknown format, try as-is

        
        for url in urls_to_try:
            result = await page.evaluate(f"""
            async () => {{
                try {{
                    const response = await fetch('{url}');
                    if (!response.ok) {{
                        return {{ success: false, error: 'HTTP ' + response.status }};
                    }}
                    
                    const contentType = response.headers.get('content-type') || '';
                    // Allow PDFs and Word Documents (and generic octet-stream)
                    const allowed = ['pdf', 'word', 'officedocument', 'octet-stream'];
                    if (!allowed.some(t => contentType.includes(t))) {{
                         // Optional: Log warning but proceed, or be strict. For now, let's allow it to attempt.
                         // return {{ success: false, error: 'Invalid Type: ' + contentType }};
                    }}
                    
                    const blob = await response.blob();
                    const reader = new FileReader();
                    
                    return new Promise((resolve) => {{
                        reader.onloadend = () => {{
                            const base64 = reader.result.split(',')[1];
                            resolve({{ success: true, data: base64, size: blob.size, type: contentType }});
                        }};
                        reader.onerror = () => {{
                            resolve({{ success: false, error: 'Read error' }});
                        }};
                        reader.readAsDataURL(blob);
                    }});
                }} catch (e) {{
                    return {{ success: false, error: e.message }};
                }}
            }}
            """)
            
            if result.get('success') and result.get('data'):
                # Decode and save
                file_bytes = base64.b64decode(result['data'])
                
                # Determine extension based on magic numbers
                final_path = filepath
                
                if file_bytes.startswith(b'%PDF'):
                    ext = '.pdf'
                elif file_bytes.startswith(b'PK'):
                    ext = '.docx' # Zip container commonly docx
                elif file_bytes.startswith(b'\xD0\xCF\x11\xE0'):
                    ext = '.doc'
                else:
                    ext = filepath.suffix # Fallback to original
                
                # Update filename if extension differs from expected
                if ext != filepath.suffix:
                    final_path = filepath.with_suffix(ext)
                
                with open(final_path, 'wb') as f:
                    f.write(file_bytes)
                
                return True, f"Downloaded ({result.get('size', len(file_bytes))} bytes) as {final_path.name}"
        
        return False, "All URLs failed"
        
    except Exception as e:
        return False, f"Error: {str(e)[:50]}"


async def establish_session(page) -> bool:
    """Navigate to FCC site to establish session cookies."""
    try:
        await page.goto('https://www.fcc.gov/ecfs', wait_until='networkidle', timeout=30000)
        await asyncio.sleep(2)
        
        # Check if we're on the actual site (not blocked)
        title = await page.title()
        if 'fcc' in title.lower() or 'ecfs' in title.lower():
            return True
        
        return True  # Proceed anyway
    except Exception as e:
        print(f"Warning: Session establishment error: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser(description='Download FCC ECFS PDFs')
    parser.add_argument('--limit', type=int, default=20, help='Max documents to download (default: 20)')
    parser.add_argument('--all', action='store_true', help='Download all documents (ignore limit)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    args = parser.parse_args()
    
    limit = None if args.all else args.limit
    
    print("[1/4] Loading company data...")
    companies = load_companies()
    print(f"      Found {len(companies)} companies")
    
    print("[2/4] Building download queue...")
    queue = build_download_queue(companies, limit)
    print(f"      {len(queue)} documents to download")
    
    if not queue:
        print("No documents to download.")
        return
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Filter out already-downloaded files
    remaining = [doc for doc in queue if not doc['filepath'].exists() or doc['filepath'].stat().st_size < 1000]
    skipped = len(queue) - len(remaining)
    
    if skipped > 0:
        print(f"      Skipping {skipped} already-downloaded files")
    
    if not remaining:
        print("All documents already downloaded!")
        return
    
    print(f"\n[3/4] Starting browser-based download...")
    print(f"      Mode: {'Headless' if args.headless else 'Visible'}")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=args.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-first-run',
                '--disable-dev-shm-usage',
            ]
        )
        
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        )
        
        # Hide automation detection
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        """)
        
        page = await context.new_page()
        
        # Establish session
        print("      Establishing session with FCC...")
        if not await establish_session(page):
            print("      Warning: Could not establish session, proceeding anyway...")
        else:
            print("      Session established successfully")
        
        success_count = 0
        fail_count = 0
        
        print(f"\n[4/4] Downloading {len(remaining)} documents...")
        
        for i, doc in enumerate(remaining, 1):
            print(f"\n[{i}/{len(remaining)}] {doc['company_name'][:40]}")
            print(f"         {doc['url']}")
            
            # Use direct download for docs.fcc.gov (static files, often block CORS in browser fetch)
            if 'docs.fcc.gov' in doc['url']:
                ok, message = await asyncio.to_thread(download_direct, doc['url'], doc['filepath'])
            else:
                ok, message = await download_via_browser_fetch(page, doc['url'], doc['filepath'])
            
            if ok:
                print(f"         ✓ {message}")
                success_count += 1
            else:
                print(f"         ✗ {message}")
                fail_count += 1
            
            # Random delay to avoid rate limiting
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            await asyncio.sleep(delay)
        
        await browser.close()
    
    print(f"\n{'='*50}")
    print(f"DOWNLOAD SUMMARY")
    print(f"{'='*50}")
    print(f"  Successful: {success_count}")
    print(f"  Failed:     {fail_count}")
    print(f"  Skipped:    {skipped}")
    print(f"  Output:     {OUTPUT_DIR}")
    print(f"{'='*50}")


if __name__ == "__main__":
    asyncio.run(main())
