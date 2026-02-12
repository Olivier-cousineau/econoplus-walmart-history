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
        return str(store_slug).strip().lower().replace(" ", "-")
    return fallback


def iter_snapshot_dates() -> list[Path]:
    date_dirs = [p for p in SNAPSHOTS_DIR.iterdir() if p.is_dir()]
    valid = []
    for path in date_dirs:
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
    today = selected[-1].name

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

                existing = bucket.get(item_key)
                if not existing:
                    existing = {
                        "title": item.get("title"),
                        "url": item.get("url"),
                        "image": item.get("image"),
                        "sku": item.get("sku"),
                        "history": [],
                        "max_30d": None,
                        "min_30d": None,
                        "last_seen": date_str,
                    }
                    bucket[item_key] = existing

                price = to_float(item.get("price_current"))
                in_stock = bool(item.get("in_stock", False))
                existing["history"].append(
                    {
                        "date": date_str,
                        "price": price,
                        "present": True,
                        "in_stock": in_stock,
                    }
                )
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
            prices = [h["price"] for h in entry["history"] if h["price"] is not None]
            max_30d = max(prices) if prices else None
            min_30d = min(prices) if prices else None
            entry["max_30d"] = max_30d
            entry["min_30d"] = min_30d
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

            if not in_stock or price_today is None or not max_30d or max_30d <= 0:
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
