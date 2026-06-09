"""
stock_sync_scrape.py
====================
Phase 2 — scrapes one chunk of listing pages SEQUENTIALLY.
Sequential = block detection and checkpoint index are always exact.

On block (BLOCK_THRESHOLD consecutive empty pages):
  1. Saves progress.json with exact resume index + partial stock_map
  2. sys.exit(2)  →  workflow saves checkpoint artifact + triggers ONE new run
                     new runner = new IP → resumes from exact stopped point

On success:
  sys.exit(0)  →  workflow does NOT trigger a new run

Usage:
    python stock_sync_scrape.py --chunk 0
    python stock_sync_scrape.py --chunk 3 --resume
"""

import argparse
import json
import re
import sys
import time
import random
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL        = "https://partworks.de"
REQ_DELAY       = (1.0, 2.5)   # polite — site won't rate limit as quickly
REQ_TIMEOUT     = 20
MAX_RETRIES     = 4
BLOCK_THRESHOLD = 50            # consecutive empty pages → blocked → exit(2)
PROGRESS_FILE   = "progress.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

logging.basicConfig(level=logging.WARNING)

_session = None

def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer":         "https://partworks.de/",
        })
    return _session

def fetch(url: str):
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(*REQ_DELAY))
            r = get_session().get(url, timeout=REQ_TIMEOUT)

            if r.status_code == 200:
                # Soft block: only flag if page is tiny AND has a block phrase
                # Real listing pages are always 30KB+
                if len(r.text) < 5000:
                    body = r.text.lower()
                    if any(x in body for x in ["captcha", "access denied", "too many requests", "rate limit", "please verify"]):
                        print(f"  [SOFT BLOCK] {url[:80]}", flush=True)
                        return None
                return BeautifulSoup(r.text, "html.parser")

            elif r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  [429] rate limited — sleeping {wait}s", flush=True)
                time.sleep(wait)

            elif r.status_code in (403, 503):
                print(f"  [HTTP {r.status_code}] hard block — {url[:80]}", flush=True)
                return None

            else:
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(3 ** attempt)
            else:
                logging.warning(f"Failed: {url} — {e}")
                return None
    return None

def scrape_page(url: str) -> dict[str, bool]:
    soup = fetch(url)
    if not soup:
        return {}
    results = {}
    for card in soup.select(".productbox.et-item-list"):
        item_num = None
        sku_el = card.select_one('[itemprop="sku"]')
        if sku_el:
            item_num = sku_el.get_text(strip=True)
        if not item_num:
            for dd in card.select(".item-detail-dd"):
                text = dd.get_text(" ", strip=True)
                if "Item number:" in text:
                    item_num = re.sub(r".*Item number:\s*", "", text).strip()
                    break
        if not item_num:
            continue
        item_num = item_num.lstrip("'").strip()
        if not re.match(r"^\d{4,8}$", item_num):
            continue

        in_stock    = False
        status_span = card.select_one(".delivery-status .status")
        if status_span:
            in_stock = "status-2" in status_span.get("class", [])
        else:
            avail = card.select_one('link[itemprop="availability"]')
            if avail:
                in_stock = "InStock" in avail.get("href", "")
        results[item_num] = in_stock
    return results

def save_progress(chunk_idx: int, resume_index: int, stock_map: dict):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"chunk": chunk_idx, "start_index": resume_index, "stock_map": stock_map}, f)
    print(f"  Checkpoint saved → resume from index {resume_index}", flush=True)

def save_results(chunk_idx: int, stock_map: dict, pages_done: int, pages_total: int, blocked: bool):
    Path("chunk_results").mkdir(exist_ok=True)
    with open(f"chunk_results/chunk_{chunk_idx:03d}.json", "w", encoding="utf-8") as f:
        json.dump({
            "chunk":       chunk_idx,
            "stock_map":   stock_map,
            "pages_done":  pages_done,
            "pages_total": pages_total,
            "blocked":     blocked,
        }, f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk",  type=int, required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    chunk_idx  = args.chunk
    chunk_file = f"page_chunks/chunk_{chunk_idx:03d}.json"

    print(f"{'=' * 65}", flush=True)
    print(f"Scraping Chunk {chunk_idx}", flush=True)
    print(f"{'=' * 65}", flush=True)

    with open(chunk_file, encoding="utf-8") as f:
        all_pages = json.load(f)

    # ── Resume from checkpoint if available ──────────────────────────
    start_index = 0
    stock_map   = {}

    if args.resume and Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        if progress.get("chunk") == chunk_idx:
            start_index = progress.get("start_index", 0)
            stock_map   = progress.get("stock_map", {})
            print(f"  Resuming from index {start_index} ({len(stock_map)} SKUs already collected)", flush=True)
        else:
            print(f"  Checkpoint is for chunk {progress.get('chunk')} — starting fresh", flush=True)

    pages = all_pages[start_index:]
    print(f"  Pages to scrape: {len(pages)} (total in chunk: {len(all_pages)})", flush=True)
    print(f"  Delay: {REQ_DELAY[0]}-{REQ_DELAY[1]}s per request", flush=True)

    empty_streak = 0

    # ── Sequential scrape — order guaranteed ─────────────────────────
    for i, url in enumerate(pages):
        abs_idx = start_index + i

        result = scrape_page(url)
        stock_map.update(result)

        if len(result) == 0:
            empty_streak += 1
        else:
            empty_streak = 0  # reset on any page that has SKUs

        if i % 50 == 0:
            print(f"  {abs_idx}/{len(all_pages)} pages | {len(stock_map)} SKUs | streak={empty_streak}", flush=True)

        # ── Block detected ────────────────────────────────────────────
        if empty_streak >= BLOCK_THRESHOLD:
            print(f"\n  ⚠ BLOCKED after {empty_streak} empty pages at index {abs_idx}", flush=True)
            print(f"  SKUs collected: {len(stock_map)}", flush=True)

            # Rewind checkpoint by BLOCK_THRESHOLD so those pages get
            # re-scraped on the fresh IP in case they were empty due to block
            resume_from = max(0, abs_idx - BLOCK_THRESHOLD + 1)
            save_progress(chunk_idx, resume_from, stock_map)
            save_results(chunk_idx, stock_map, abs_idx, len(all_pages), blocked=True)

            print(f"  sys.exit(2) → workflow will trigger ONE new run to resume", flush=True)
            sys.exit(2)  # ← workflow checks this exact code — NOT exit(1)

    # ── Clean finish ──────────────────────────────────────────────────
    in_s = sum(1 for v in stock_map.values() if v)
    print(f"\n  ✓ Chunk {chunk_idx} done: {len(stock_map)} SKUs ({in_s} in stock)", flush=True)
    print(f"  Pages: {len(all_pages)}/{len(all_pages)}", flush=True)

    save_results(chunk_idx, stock_map, len(all_pages), len(all_pages), blocked=False)

    if Path(PROGRESS_FILE).exists():
        Path(PROGRESS_FILE).unlink()

    sys.exit(0)  # ← workflow does NOT trigger a new run on exit(0)

if __name__ == "__main__":
    main()
