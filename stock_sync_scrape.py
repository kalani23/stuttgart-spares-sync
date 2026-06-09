"""
stock_sync_scrape.py
====================
Scrapes one chunk of listing pages SEQUENTIALLY so block detection
and checkpoint index are always accurate.

Block detection: if BLOCK_THRESHOLD consecutive pages return 0 SKUs:
  1. Saves progress.json checkpoint (exact page index + partial stock_map)
  2. sys.exit(2) → workflow uploads checkpoint artifact + self-triggers
     a new workflow run → new runner = new IP → resumes from checkpoint

Usage:
    python stock_sync_scrape.py --chunk 0
    python stock_sync_scrape.py --chunk 3 --resume   # resume from progress.json

Outputs:
    chunk_results/chunk_003.json
    progress.json  (only on block)
"""

import argparse
import json
import re
import time
import random
import sys
import logging
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL        = "https://partworks.de"
REQ_DELAY       = (0.8, 2.0)
REQ_TIMEOUT     = 15
MAX_RETRIES     = 3
BLOCK_THRESHOLD = 50    # consecutive empty pages → blocked → exit(2)
PROGRESS_FILE   = "progress.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

logging.basicConfig(level=logging.WARNING)

# One session for the whole run — sequential so no threading needed
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
                body = r.text.lower()
                if any(x in body for x in ["captcha", "access denied", "too many requests", "blocked"]):
                    print(f"  [SOFT BLOCK] {url[:80]}", flush=True)
                    return None
                return BeautifulSoup(r.text, "html.parser")
            elif r.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"  [429] rate limited — sleeping {wait}s", flush=True)
                time.sleep(wait)
            elif r.status_code in (403, 503):
                print(f"  [HTTP {r.status_code}] hard block on {url[:80]}", flush=True)
                return None
            else:
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                logging.warning(f"Failed: {url} — {e}")
                return None
    return None

def scrape_listing_page(url: str) -> dict[str, bool]:
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

        in_stock = False
        status_span = card.select_one(".delivery-status .status")
        if status_span:
            classes = status_span.get("class", [])
            in_stock = "status-2" in classes
        else:
            badge = card.select_one(".label-success, .label-danger, .availability")
            if badge:
                in_stock = "stock" in badge.get_text(strip=True).lower()

        results[item_num] = in_stock
    return results

def save_progress(chunk_idx: int, resume_index: int, stock_map: dict):
    """Save checkpoint. resume_index = the page index to START from next run."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "chunk":        chunk_idx,
            "start_index":  resume_index,
            "stock_map":    stock_map,
        }, f)
    print(f"  Checkpoint saved → {PROGRESS_FILE} (next run resumes from index {resume_index})", flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk",  type=int, required=True)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    chunk_idx  = args.chunk
    chunk_file = f"page_chunks/chunk_{chunk_idx:03d}.json"

    print(f"{'=' * 65}", flush=True)
    print(f"Stuttgart Spares — Scraping Chunk {chunk_idx}", flush=True)
    print(f"{'=' * 65}", flush=True)

    with open(chunk_file, encoding="utf-8") as f:
        all_pages = json.load(f)

    # ── Resume from checkpoint ────────────────────────────────────────
    start_index = 0
    stock_map   = {}

    if args.resume and Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        if progress.get("chunk") == chunk_idx:
            start_index = progress.get("start_index", 0)
            stock_map   = progress.get("stock_map", {})
            print(f"  Resuming from page index {start_index} ({len(stock_map)} SKUs already collected)", flush=True)
        else:
            print(f"  Checkpoint is for chunk {progress.get('chunk')}, not {chunk_idx} — starting fresh", flush=True)

    pages = all_pages[start_index:]
    print(f"  Pages to scrape: {len(pages)} (of {len(all_pages)} total)", flush=True)
    print(f"  Delay: {REQ_DELAY[0]}-{REQ_DELAY[1]}s per request", flush=True)

    empty_streak = 0

    # ── Sequential scrape — order guaranteed, checkpoint exact ───────
    for i, url in enumerate(pages):
        abs_idx = start_index + i   # absolute position in all_pages

        result       = scrape_listing_page(url)
        stock_map.update(result)

        if len(result) == 0:
            empty_streak += 1
        else:
            empty_streak  = 0   # reset on any page that has SKUs

        if i % 50 == 0:
            print(f"  {abs_idx}/{len(all_pages)} pages | {len(stock_map)} SKUs | empty_streak={empty_streak}", flush=True)

        if empty_streak >= BLOCK_THRESHOLD:
            print(f"\n  ⚠ BLOCKED: {empty_streak} consecutive empty pages", flush=True)
            print(f"  SKUs collected: {len(stock_map)} | Stopped at index {abs_idx}", flush=True)

            # Save checkpoint — next run starts from the FIRST empty page in this streak
            # so we don't skip pages that might have been empty due to block not category
            resume_from = abs_idx - BLOCK_THRESHOLD + 1
            save_progress(chunk_idx, resume_from, stock_map)

            # Save partial results so merge job still uses them
            Path("chunk_results").mkdir(exist_ok=True)
            with open(f"chunk_results/chunk_{chunk_idx:03d}.json", "w", encoding="utf-8") as f:
                json.dump({
                    "chunk":        chunk_idx,
                    "stock_map":    stock_map,
                    "pages_done":   abs_idx,
                    "pages_total":  len(all_pages),
                    "blocked":      True,
                }, f)

            print(f"  Exiting with code 2 → workflow will self-trigger a new run", flush=True)
            sys.exit(2)

    # ── Clean finish ──────────────────────────────────────────────────
    in_s  = sum(1 for v in stock_map.values() if v)
    out_s = len(stock_map) - in_s
    print(f"\n  ✓ Chunk {chunk_idx} complete", flush=True)
    print(f"  SKUs: {len(stock_map)} ({in_s} in stock, {out_s} out of stock)", flush=True)
    print(f"  Pages: {len(all_pages)}/{len(all_pages)}", flush=True)

    Path("chunk_results").mkdir(exist_ok=True)
    with open(f"chunk_results/chunk_{chunk_idx:03d}.json", "w", encoding="utf-8") as f:
        json.dump({
            "chunk":        chunk_idx,
            "stock_map":    stock_map,
            "pages_done":   len(all_pages),
            "pages_total":  len(all_pages),
            "blocked":      False,
        }, f)

    # Clean up checkpoint file on successful finish
    if Path(PROGRESS_FILE).exists():
        Path(PROGRESS_FILE).unlink()

    sys.exit(0)

if __name__ == "__main__":
    main()
