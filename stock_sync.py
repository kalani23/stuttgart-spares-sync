"""
stock_sync.py  v3 — Full combined stock sync
=============================================
One script does everything:
  1. Scrapes all partworks categories (listing pages only, no product visits)
  2. Builds SKU → in_stock map
  3. Fetches Shopify products
  4. Updates inventory where changed

Schedule daily via Windows Task Scheduler.

Usage:
    python stock_sync.py             # full live run
    python stock_sync.py --dry-run   # scrape + show changes, no Shopify updates
"""

import json
import re
import sys
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

SHOPIFY_TOKEN  = "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"
SHOP           = "27dkze-zv.myshopify.com"
API_VERSION    = "2024-01"
STOCK_MAP_FILE = "stock_map.json"
SYNC_LOG_FILE  = "sync_log.json"

DRY_RUN     = "--dry-run" in sys.argv
MAX_WORKERS = 16
REQ_DELAY   = (0.2, 0.6)
REQ_TIMEOUT = 15
MAX_RETRIES = 3

BASE_URL        = "https://partworks.de"
SHOPIFY_BASE    = f"https://{SHOP}/admin/api/{API_VERSION}"
SHOPIFY_HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

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

# ── Thread-safe helpers ───────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.WARNING,
    handlers=[logging.FileHandler("stock_sync_debug.log", encoding="utf-8")]
)

print_lock = Lock()
def tprint(*args):
    with print_lock:
        print(*args, flush=True)

import threading
_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_local, "s"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
                wait = 15 * (attempt + 1)
                tprint(f"  [429] rate limited — sleeping {wait}s")
                time.sleep(wait)
            else:
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                logging.warning(f"Failed: {url} — {e}")
                return None
    return None

# ═════════════════════════════════════════════════════════════════════════════
# PART 1 — SCRAPE PARTWORKS
# ═════════════════════════════════════════════════════════════════════════════

def get_subcats_for_category(category: str) -> list[str]:
    soup = fetch(BASE_URL + category)
    if not soup:
        return []
    urls = [a.get("href","") for a in soup.select(".et-sub-category a.et-sub-category-link-wrapper") if a.get("href")]
    return urls if urls else [BASE_URL + category]

def get_all_subcats() -> list[str]:
    print("\n── Phase 1: Discovering subcategories ──────────────────────")
    all_urls, seen, lock = [], set(), Lock()

    def fetch_cat(cat):
        urls = get_subcats_for_category(cat)
        with lock:
            new = [u for u in urls if u not in seen]
            seen.update(new)
            all_urls.extend(new)
            tprint(f"  {cat:<50} → {len(new)} subcategories")

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(as_completed([ex.submit(fetch_cat, c) for c in ALL_CATEGORIES]))

    print(f"\n  Total subcategories: {len(all_urls)}")
    return all_urls

def get_listing_pages(subcat_url: str) -> list[str]:
    soup = fetch(subcat_url)
    if not soup:
        return [subcat_url]
    pages = [subcat_url]
    for a in soup.select(".navbar-pagination a.page-link"):
        href = a.get("href","").split("#")[0]
        if href:
            full = href if href.startswith("http") else BASE_URL + href
            if full not in pages:
                pages.append(full)
    return pages

def get_all_listing_pages(subcat_urls: list[str]) -> list[str]:
    print("\n── Phase 2: Collecting listing pages ───────────────────────")
    all_pages, seen, lock, done = [], set(), Lock(), [0]

    def collect(url):
        pages = get_listing_pages(url)
        with lock:
            for p in pages:
                if p not in seen:
                    seen.add(p)
                    all_pages.append(p)
            done[0] += 1
            if done[0] % 100 == 0:
                tprint(f"  {done[0]}/{len(subcat_urls)} subcats | {len(all_pages)} pages")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(as_completed([ex.submit(collect, u) for u in subcat_urls]))

    print(f"  Total listing pages: {len(all_pages)}")
    return all_pages

def scrape_listing_page(url: str) -> dict[str, bool]:
    soup = fetch(url)
    if not soup:
        return {}
    results = {}
    for card in soup.select(".productbox.et-item-list"):
        # Get item number
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
        # Get stock status — check CSS class directly
        in_stock = False
        status_span = card.select_one(".delivery-status .status")
        if status_span:
            classes = status_span.get("class", [])
            in_stock = "status-2" in classes
        else:
            avail = card.select_one('link[itemprop="availability"]')
            if avail:
                in_stock = "InStock" in avail.get("href", "")
        results[item_num] = in_stock
    return results

def scrape_all_stock(listing_pages: list[str]) -> dict[str, bool]:
    print(f"\n── Phase 3: Scraping stock ({MAX_WORKERS} workers) ──────────────────")
    stock_map, lock, done = {}, Lock(), [0]

    def scrape(url):
        result = scrape_listing_page(url)
        with lock:
            stock_map.update(result)
            done[0] += 1
            if done[0] % 200 == 0:
                tprint(f"  {done[0]}/{len(listing_pages)} pages | {len(stock_map)} SKUs")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        list(as_completed([ex.submit(scrape, u) for u in listing_pages]))

    in_s  = sum(1 for v in stock_map.values() if v)
    out_s = len(stock_map) - in_s
    print(f"\n  ✓ Scrape complete: {len(stock_map)} SKUs | {in_s} in stock | {out_s} out of stock")
    return stock_map

# ═════════════════════════════════════════════════════════════════════════════
# PART 2 — UPDATE SHOPIFY
# ═════════════════════════════════════════════════════════════════════════════

def get_shopify_products() -> list[dict]:
    print("\n── Fetching Shopify products ────────────────────────────────")
    products = []
    url = f"{SHOPIFY_BASE}/products.json?limit=250&fields=id,variants"
    while url:
        r    = requests.get(url, headers=SHOPIFY_HEADERS, timeout=20)
        data = r.json()
        for product in data.get("products", []):
            for variant in product.get("variants", []):
                sku = (variant.get("sku") or "").strip().lstrip("'")
                if not re.match(r"^\d{4,8}$", sku):
                    continue
                products.append({
                    "product_id":        product["id"],
                    "variant_id":        variant["id"],
                    "sku":               sku,
                    "inventory_item_id": variant.get("inventory_item_id"),
                    "current_qty":       variant.get("inventory_quantity", 0),
                })
        link = r.headers.get("Link", "")
        url  = None
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.strip().split(";")[0].strip("<>")
    print(f"  Found {len(products)} partworks products on Shopify")
    return products

def get_location_id(products: list[dict]) -> int | None:
    """Detect location ID from an existing inventory level — no locations scope needed."""
    for product in products[:10]:
        iid = product.get("inventory_item_id")
        if not iid:
            continue
        r = requests.get(
            f"{SHOPIFY_BASE}/inventory_levels.json?inventory_item_ids={iid}",
            headers=SHOPIFY_HEADERS, timeout=20
        )
        levels = r.json().get("inventory_levels", [])
        if levels:
            lid = levels[0]["location_id"]
            print(f"  Location ID: {lid} (auto-detected)")
            return lid
    return None

def ensure_tracking(variant_id: int):
    requests.put(
        f"{SHOPIFY_BASE}/variants/{variant_id}.json",
        headers=SHOPIFY_HEADERS,
        json={"variant": {"id": variant_id, "inventory_management": "shopify"}},
        timeout=20
    )

def set_inventory_level(inventory_item_id: int, location_id: int, qty: int) -> bool:
    r = requests.post(
        f"{SHOPIFY_BASE}/inventory_levels/set.json",
        headers=SHOPIFY_HEADERS,
        json={"location_id": location_id, "inventory_item_id": inventory_item_id, "available": qty},
        timeout=20
    )
    if r.status_code != 200:
        tprint(f"    [API ERROR {r.status_code}] {r.text[:150]}")
        return False
    return True

def update_shopify(stock_map: dict[str, bool], products: list[dict], location_id: int):
    print("\n── Updating Shopify inventory ───────────────────────────────")

    changes, no_change, not_found, updated, errors = [], 0, 0, 0, 0

    for i, product in enumerate(products, 1):
        sku               = product["sku"]
        current_qty       = product["current_qty"]
        variant_id        = product["variant_id"]
        inventory_item_id = product["inventory_item_id"]

        if sku not in stock_map:
            not_found += 1
            continue

        target_qty = 2 if stock_map[sku] else 0

        if current_qty == target_qty:
            no_change += 1
            continue

        direction = "IN STOCK  ↑" if stock_map[sku] else "OUT STOCK ↓"
        tprint(f"  [{i}/{len(products)}] SKU {sku:<8} {current_qty} → {target_qty}  [{direction}]")

        changes.append({
            "sku":        sku,
            "product_id": product["product_id"],
            "from_qty":   current_qty,
            "to_qty":     target_qty,
            "in_stock":   stock_map[sku],
            "timestamp":  datetime.now().isoformat(),
        })

        if not DRY_RUN:
            ensure_tracking(variant_id)
            time.sleep(0.2)
            if set_inventory_level(inventory_item_id, location_id, target_qty):
                updated += 1
            else:
                errors += 1
            time.sleep(0.4)
        else:
            updated += 1

    return changes, no_change, not_found, updated, errors

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    start = time.time()

    print("=" * 65)
    print("Stuttgart Spares — Daily Stock Sync")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode:    {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print("=" * 65)

    # ── Scrape partworks ──────────────────────────────────────────────
    subcat_urls   = get_all_subcats()
    listing_pages = get_all_listing_pages(subcat_urls)
    stock_map     = scrape_all_stock(listing_pages)

    # Save stock map
    with open(STOCK_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(stock_map, f, indent=2)
    print(f"\n  Stock map saved → {STOCK_MAP_FILE}")

    # ── Fetch Shopify products ────────────────────────────────────────
    products = get_shopify_products()

    # ── Get location ID ───────────────────────────────────────────────
    location_id = None
    if not DRY_RUN:
        print("\n── Getting location ID ──────────────────────────────────────")
        location_id = get_location_id(products)
        if not location_id:
            print("  [WARN] Could not auto-detect location ID.")
            location_id = int(input("  Paste location ID manually: ").strip())

    # ── Update Shopify ────────────────────────────────────────────────
    changes, no_change, not_found, updated, errors = update_shopify(
        stock_map, products, location_id
    )

    # ── Save log ──────────────────────────────────────────────────────
    elapsed = time.time() - start
    run_record = {
        "run_at":                 datetime.now().isoformat(),
        "dry_run":                DRY_RUN,
        "elapsed_sec":            round(elapsed, 1),
        "location_id":            location_id,
        "partworks_skus":         len(stock_map),
        "shopify_products":       len(products),
        "not_found_on_partworks": not_found,
        "no_change":              no_change,
        "updated":                updated,
        "errors":                 errors,
        "changes":                changes,
    }
    log_path = Path(SYNC_LOG_FILE)
    all_logs = json.loads(log_path.read_text(encoding="utf-8")) if log_path.exists() else []
    all_logs.append(run_record)
    log_path.write_text(json.dumps(all_logs[-30:], indent=2, ensure_ascii=False), encoding="utf-8")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print(f"{'DRY RUN COMPLETE' if DRY_RUN else 'SYNC COMPLETE'}")
    print(f"  Elapsed:          {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Partworks SKUs:   {len(stock_map)}")
    print(f"  Shopify products: {len(products)}")
    print(f"  No change:        {no_change}")
    print(f"  Not on partworks: {not_found}")
    print(f"  {'Would update' if DRY_RUN else 'Updated'}:        {updated}")
    if errors:
        print(f"  Errors:           {errors}")
    print(f"  Log:              {SYNC_LOG_FILE}")
    print(f"  Stock map:        {STOCK_MAP_FILE}")
    if DRY_RUN:
        print(f"\n  Run without --dry-run to apply changes.")
    print("=" * 65)


if __name__ == "__main__":
    main()
