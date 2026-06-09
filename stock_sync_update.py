"""
stock_sync_update.py
====================
Phase 3: Merges all chunk results into a single stock_map,
then updates Shopify inventory for changed products.

Run after all scrape jobs complete.
"""

import json
import os
import re
import time
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock

import requests

# ── Config ────────────────────────────────────────────────────────────────────

SHOPIFY_TOKEN  = os.environ.get("SHOPIFY_TOKEN", "")
SHOP           = "27dkze-zv.myshopify.com"
API_VERSION    = "2024-01"
STOCK_MAP_FILE = "stock_map.json"
SYNC_LOG_FILE  = "sync_log.json"

SHOPIFY_BASE    = f"https://{SHOP}/admin/api/{API_VERSION}"
SHOPIFY_HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

print_lock = Lock()
def tprint(*args):
    with print_lock:
        print(*args, flush=True)

# ── Merge chunk results ───────────────────────────────────────────────────────

def merge_chunks() -> tuple[dict, dict]:
    """Merge all chunk_results/*.json into one stock_map. Returns (stock_map, stats)."""
    chunk_dir = Path("chunk_results")
    files     = sorted(chunk_dir.glob("chunk_*.json"))

    if not files:
        raise FileNotFoundError("No chunk result files found in chunk_results/")

    merged      = {}
    total_pages = 0
    done_pages  = 0
    blocked_chunks = []

    print(f"\n── Merging {len(files)} chunk results ───────────────────────────")

    for f in files:
        with open(f, encoding="utf-8") as fp:
            data = json.load(fp)

        chunk_map   = data.get("stock_map", {})
        chunk_idx   = data.get("chunk", "?")
        pages_done  = data.get("pages_done", 0)
        pages_total = data.get("pages_total", 0)
        was_blocked = data.get("blocked", False)

        merged.update(chunk_map)
        done_pages  += pages_done
        total_pages += pages_total

        status = "⚠ BLOCKED" if was_blocked else "✓"
        print(f"  {status} chunk_{chunk_idx:03d}: {len(chunk_map)} SKUs | {pages_done}/{pages_total} pages")

        if was_blocked:
            blocked_chunks.append(chunk_idx)

    in_s  = sum(1 for v in merged.values() if v)
    out_s = len(merged) - in_s
    print(f"\n  Total merged: {len(merged)} SKUs ({in_s} in stock, {out_s} out)")
    print(f"  Pages:        {done_pages}/{total_pages}")
    if blocked_chunks:
        print(f"  ⚠ Blocked chunks (partial data): {blocked_chunks}")

    stats = {
        "total_skus":      len(merged),
        "pages_done":      done_pages,
        "pages_total":     total_pages,
        "blocked_chunks":  blocked_chunks,
        "chunks_total":    len(files),
    }
    return merged, stats

# ── Shopify helpers ───────────────────────────────────────────────────────────

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

def update_shopify(stock_map, products, location_id):
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

        ensure_tracking(variant_id)
        time.sleep(0.2)
        if set_inventory_level(inventory_item_id, location_id, target_qty):
            updated += 1
        else:
            errors += 1
        time.sleep(0.4)

    return changes, no_change, not_found, updated, errors

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = time.time()

    print("=" * 65)
    print("Stuttgart Spares — Merge & Shopify Update")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # Merge
    stock_map, scrape_stats = merge_chunks()

    # Save stock map
    with open(STOCK_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(stock_map, f, indent=2)
    print(f"\n  Stock map saved → {STOCK_MAP_FILE}")

    # Shopify
    products    = get_shopify_products()
    location_id = get_location_id(products)
    if not location_id:
        print("  [ERROR] Could not detect location ID — aborting Shopify update")
        return

    changes, no_change, not_found, updated, errors = update_shopify(
        stock_map, products, location_id
    )

    # Log
    elapsed = time.time() - start
    run_record = {
        "run_at":                 datetime.now().isoformat(),
        "elapsed_sec":            round(elapsed, 1),
        "location_id":            location_id,
        "partworks_skus":         scrape_stats["total_skus"],
        "pages_done":             scrape_stats["pages_done"],
        "pages_total":            scrape_stats["pages_total"],
        "blocked_chunks":         scrape_stats["blocked_chunks"],
        "chunks_total":           scrape_stats["chunks_total"],
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

    # Summary
    print(f"\n{'=' * 65}")
    print(f"SYNC COMPLETE")
    print(f"  Elapsed:          {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Partworks SKUs:   {scrape_stats['total_skus']}")
    print(f"  Pages scraped:    {scrape_stats['pages_done']}/{scrape_stats['pages_total']}")
    if scrape_stats["blocked_chunks"]:
        print(f"  ⚠ Blocked chunks: {scrape_stats['blocked_chunks']}")
    print(f"  Shopify products: {len(products)}")
    print(f"  No change:        {no_change}")
    print(f"  Not on partworks: {not_found}")
    print(f"  Updated:          {updated}")
    if errors:
        print(f"  Errors:           {errors}")
    print("=" * 65)

if __name__ == "__main__":
    main()
