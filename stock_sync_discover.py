"""
stock_sync_discover.py
======================
Phase 1 — discovers all subcategories and listing pages,
splits into chunks of CHUNK_SIZE pages each.

Outputs:
    page_chunks/chunk_000.json, chunk_001.json ...
    chunk_count.txt  e.g. [0,1,2,3,4]
"""

import json
import re
import time
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://partworks.de"
CHUNK_SIZE  = 300        # pages per chunk — each chunk = 1 runner = fresh IP
MAX_WORKERS = 6          # polite during discovery
REQ_DELAY   = (1.0, 2.5)
REQ_TIMEOUT = 20
MAX_RETRIES = 4

ALL_CATEGORIES = [
    "/Porsche/356-spare-parts",
    "/Porsche/911-F-Model-Spare-Parts",
    "/Porsche/912-spare-parts",
    "/Porsche/911-G-Model-Spare-Parts",
    "/Porsche/964-spare-parts",
    "/Porsche/993-spare-parts",
    "/Porsche/996-spare-parts",
    "/Porsche/997-spare-parts",
    "/Porsche/991-spare-parts",
    "/Porsche/914-spare-parts",
    "/Porsche/944-spare-parts",
    "/Porsche/924-spare-parts",
    "/Porsche/968-spare-parts",
    "/Porsche/928-spare-parts",
    "/Porsche/Boxster-986-spare-parts",
    "/Porsche/Boxster-Cayman-987-spare-parts",
    "/Porsche/Boxster-Cayman-981-spare-parts",
    "/Porsche/Cayenne-955-spare-parts",
    "/Porsche/Cayenne-957-spare-parts",
    "/Porsche/Cayenne-958-spare-parts",
    "/Porsche/Panamera-970-spare-parts",
    "/Porsche/Panamera-970FL-spare-parts",
    "/Porsche/Macan-95B-spare-parts",
    "/Porsche/Spare-parts-for-newer-models",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

logging.basicConfig(level=logging.WARNING)
print_lock = Lock()
def tprint(*args): 
    with print_lock: 
        print(*args, flush=True)

import threading
_local = threading.local()

def get_session():
    if not hasattr(_local, "s"):
        s = requests.Session()
        s.headers.update({
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer":         "https://partworks.de/",
        })
        _local.s = s
    return _local.s

def fetch(url: str):
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(*REQ_DELAY))
            r = get_session().get(url, timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "html.parser")
            elif r.status_code == 429:
                wait = 30 * (attempt + 1)
                tprint(f"  [429] sleeping {wait}s")
                time.sleep(wait)
            else:
                tprint(f"  [HTTP {r.status_code}] {url[:70]}")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(3 ** attempt)
            else:
                logging.warning(f"Failed: {url} — {e}")
                return None
    return None

def get_subcats(category: str) -> list[str]:
    soup = fetch(BASE_URL + category)
    if not soup:
        return [BASE_URL + category]
    urls = [a.get("href", "") for a in soup.select(".et-sub-category a.et-sub-category-link-wrapper") if a.get("href")]
    return urls if urls else [BASE_URL + category]

def get_pages(subcat_url: str) -> list[str]:
    soup = fetch(subcat_url)
    if not soup:
        return [subcat_url]
    pages = [subcat_url]
    for a in soup.select(".navbar-pagination a.page-link"):
        href = a.get("href", "").split("#")[0]
        if href:
            full = href if href.startswith("http") else BASE_URL + href
            if full not in pages:
                pages.append(full)
    return pages

def main():
    print("=" * 65)
    print("Stuttgart Spares — Discovery Phase")
    print("=" * 65)

    # Phase 1 — subcategories
    print("\n── Phase 1: Subcategories ───────────────────────────────────")
    all_subcats, seen, lock = [], set(), Lock()

    def fetch_cat(cat):
        urls = get_subcats(cat)
        with lock:
            new = [u for u in urls if u not in seen]
            seen.update(new)
            all_subcats.extend(new)
            tprint(f"  {cat:<50} → {len(new)} subcats")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(as_completed([ex.submit(fetch_cat, c) for c in ALL_CATEGORIES]))

    print(f"\n  Total subcategories: {len(all_subcats)}")

    # Phase 2 — listing pages
    print("\n── Phase 2: Listing pages ───────────────────────────────────")
    all_pages, seen2, lock2, done = [], set(), Lock(), [0]

    def collect(url):
        pages = get_pages(url)
        with lock2:
            for p in pages:
                if p not in seen2:
                    seen2.add(p)
                    all_pages.append(p)
            done[0] += 1
            if done[0] % 100 == 0:
                tprint(f"  {done[0]}/{len(all_subcats)} subcats | {len(all_pages)} pages")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(as_completed([ex.submit(collect, u) for u in all_subcats]))

    print(f"  Total listing pages: {len(all_pages)}")

    # Phase 3 — split into chunks
    print(f"\n── Splitting into chunks of {CHUNK_SIZE} pages ─────────────────")
    Path("page_chunks").mkdir(exist_ok=True)

    chunks  = [all_pages[i:i + CHUNK_SIZE] for i in range(0, len(all_pages), CHUNK_SIZE)]
    indices = list(range(len(chunks)))

    for i, chunk in enumerate(chunks):
        with open(f"page_chunks/chunk_{i:03d}.json", "w", encoding="utf-8") as f:
            json.dump(chunk, f)
        print(f"  chunk_{i:03d}.json → {len(chunk)} pages")

    with open("chunk_count.txt", "w") as f:
        f.write(json.dumps(indices))

    print(f"\n  ✓ {len(chunks)} chunks ready → chunk_count={json.dumps(indices)}")

if __name__ == "__main__":
    main()
