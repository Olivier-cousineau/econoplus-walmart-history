#!/usr/bin/env python3
"""Build Walmart historical indexes and daily drop deals."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SNAPSHOTS_DIR = Path("snapshots")
INDEXES_DIR = Path("indexes")


def parse_date(folder_name: str) -> datetime:
    return datetime.strptime(folder_name, "%Y-%m-%d")


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown-store"


def load_items(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return [item for item in data["items"] if isinstance(item, dict)]
    return []


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("$", "").replace(",", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def extract_url_id(url: str | None) -> str | None:
    if not url:
        return None

    patterns = [
        r"/ip/.+/(\d{6,})",
        r"/(\d{8,})",
        r"/([A-Z0-9]{8,})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def make_item_key(item: dict[str, Any]) -> str:
    sku = item.get("sku")
    if sku not in (None, ""):
        return f"sku:{sku}"

    url_id = extract_url_id(item.get("url"))
    if url_id:
        return f"urlid:{url_id}"

    title = str(item.get("title") or "")
    image = str(item.get("image") or "")
    digest = hashlib.sha1(f"{title}|{image}".encode("utf-8")).hexdigest()[:16]
    return f"hash:{digest}"


def normalize_store_slug(item: dict[str, Any], fallback: str) -> str:
    store_slug = item.get("store_slug") or item.get("store")
    if store_slug:
        return slugify(str(store_slug))
    return fallback


def iter_snapshot_dates() -> list[Path]:
    if not SNAPSHOTS_DIR.exists():
        return []
    valid = []
    for path in SNAPSHOTS_DIR.iterdir():
        if not path.is_dir():
            continue
        try:
            parse_date(path.name)
            valid.append(path)
        except ValueError:
            continue
    return sorted(valid, key=lambda p: p.name)


def build_history(days: int) -> tuple[dict[str, dict[str, Any]], str]:
    dates = iter_snapshot_dates()
    if not dates:
        return {}, ""

    selected = dates[-days:]
    selected_dates = [d.name for d in selected]
    today = selected_dates[-1]

    stores: dict[str, dict[str, Any]] = defaultdict(lambda: {"items": {}})

    for date_dir in selected:
        date_str = date_dir.name
        for store_file in sorted(date_dir.glob("*.json")):
            if store_file.name == "walmart_all.json":
                continue

            file_store_slug = store_file.stem
            items = load_items(store_file)

            for item in items:
                store_slug = normalize_store_slug(item, file_store_slug)
                bucket = stores[store_slug]["items"]
                item_key = make_item_key(item)

                if item_key not in bucket:
                    bucket[item_key] = {
                        "title": item.get("title"),
                        "url": item.get("url"),
                        "image": item.get("image"),
                        "sku": item.get("sku"),
                        "history": {},
                        "max_30d": None,
                        "min_30d": None,
                        "last_seen": date_str,
                    }

                existing = bucket[item_key]
                price = to_float(item.get("price_current"))
                in_stock = bool(item.get("in_stock", False))
                existing["history"][date_str] = {
                    "date": date_str,
                    "price": price,
                    "present": True,
                    "in_stock": in_stock,
                }
                existing["last_seen"] = date_str
                existing["title"] = item.get("title") or existing.get("title")
                existing["url"] = item.get("url") or existing.get("url")
                existing["image"] = item.get("image") or existing.get("image")
                existing["sku"] = item.get("sku") or existing.get("sku")

    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    history_output: dict[str, dict[str, Any]] = {}
    for store_slug, store_data in stores.items():
        items_out: dict[str, Any] = {}
        for item_key, entry in store_data["items"].items():
            for date_str in selected_dates:
                if date_str not in entry["history"]:
                    entry["history"][date_str] = {
                        "date": date_str,
                        "price": None,
                        "in_stock": False,
                        "present": False,
                    }

            ordered_history = [entry["history"][date_str] for date_str in selected_dates]
            prices = [h["price"] for h in ordered_history if h["present"] and h["price"] is not None]

            entry["history"] = ordered_history
            entry["max_30d"] = max(prices) if prices else None
            entry["min_30d"] = min(prices) if prices else None
            items_out[item_key] = entry

        history_output[store_slug] = {
            "store_slug": store_slug,
            "updated_at": updated_at,
            "items": items_out,
        }

    return history_output, today


def write_history_indexes(history_output: dict[str, dict[str, Any]]) -> None:
    out_dir = INDEXES_DIR / "history_store"
    out_dir.mkdir(parents=True, exist_ok=True)

    for store_slug, payload in history_output.items():
        out_file = out_dir / f"{store_slug}.json"
        with out_file.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.write("\n")


def build_deals(today: str, history_output: dict[str, dict[str, Any]], drop_threshold: float) -> dict[str, Any]:
    deals_by_store: dict[str, Any] = {}
    if not today:
        return deals_by_store

    date_dir = SNAPSHOTS_DIR / today

    for store_file in sorted(date_dir.glob("*.json")):
        if store_file.name == "walmart_all.json":
            continue

        store_slug = store_file.stem
        items = load_items(store_file)
        history_slug = normalize_store_slug(items[0], store_slug) if items else store_slug
        history_items = history_output.get(history_slug, {}).get("items", {})
        deals = []

        for item in items:
            item_key = make_item_key(item)
            history_entry = history_items.get(item_key)
            if not history_entry:
                continue

            price_today = to_float(item.get("price_current"))
            max_30d = history_entry.get("max_30d")
            in_stock = bool(item.get("in_stock", False))

            if not in_stock or price_today is None or max_30d is None or max_30d <= 0:
                continue

            drop_pct = ((max_30d - price_today) / max_30d) * 100.0
            if drop_pct >= drop_threshold:
                deals.append(
                    {
                        "item_key": item_key,
                        "sku": item.get("sku"),
                        "title": item.get("title"),
                        "price_today": price_today,
                        "max_30d": max_30d,
                        "drop_pct": round(drop_pct, 2),
                        "in_stock": in_stock,
                        "url": item.get("url"),
                        "image": item.get("image"),
                    }
                )

        deals_by_store[history_slug] = {
            "date": today,
            "store_slug": history_slug,
            "deals": sorted(deals, key=lambda d: d["drop_pct"], reverse=True),
        }

    return deals_by_store


def write_deals(today: str, deals_by_store: dict[str, Any]) -> None:
    out_dir = INDEXES_DIR / "deals_daily" / today
    out_dir.mkdir(parents=True, exist_ok=True)

    for store_slug, payload in deals_by_store.items():
        out_file = out_dir / f"{store_slug}.json"
        with out_file.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build store history indexes and daily deals indexes.")
    parser.add_argument("--days", type=int, default=30, help="Number of days to include from snapshots/.")
    parser.add_argument("--drop", type=float, default=15.0, help="Drop percentage threshold for deals.")
    args = parser.parse_args()

    history_output, today = build_history(days=args.days)
    if not history_output:
        print("No valid snapshots found in snapshots/YYYY-MM-DD; nothing to index.")
        return

    write_history_indexes(history_output)
    deals_by_store = build_deals(today=today, history_output=history_output, drop_threshold=args.drop)
    write_deals(today=today, deals_by_store=deals_by_store)

    print(
        f"Built history indexes for {len(history_output)} stores and deals for {len(deals_by_store)} stores (date={today})."
    )


if __name__ == "__main__":
    main()
