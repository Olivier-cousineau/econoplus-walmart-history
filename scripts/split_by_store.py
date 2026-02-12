#!/usr/bin/env python3
"""Split a Walmart snapshot file into one file per store."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "unknown-store"


def load_items(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return [item for item in data["items"] if isinstance(item, dict)]

    raise ValueError(f"Unsupported input format in {path}")


def infer_captured_at(item: dict[str, Any], fallback_date: str) -> str:
    captured = item.get("captured_at")
    if isinstance(captured, str) and captured.strip():
        return captured
    return f"{fallback_date}T00:00:00Z"


def group_by_store(items: Iterable[dict[str, Any]], fallback_date: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for raw_item in items:
        item = dict(raw_item)
        store_val = item.get("store_slug") or item.get("store")
        store_slug = slugify(str(store_val)) if store_val else "unknown-store"
        item["store_slug"] = store_slug
        item["captured_at"] = infer_captured_at(item, fallback_date)
        grouped[store_slug].append(item)

    return grouped


def main() -> None:
    parser = argparse.ArgumentParser(description="Split walmart_all.json into one file per store.")
    parser.add_argument(
        "--input",
        default=None,
        help="Path to snapshots/YYYY-MM-DD/walmart_all.json (default: latest walmart_all.json found).",
    )
    args = parser.parse_args()

    if args.input:
        input_path = Path(args.input)
    else:
        candidates = sorted(Path("snapshots").glob("*/walmart_all.json"))
        if not candidates:
            raise FileNotFoundError("No snapshots/*/walmart_all.json file found.")
        input_path = candidates[-1]

    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    try:
        fallback_date = input_path.parent.name
        datetime.strptime(fallback_date, "%Y-%m-%d")
    except ValueError:
        fallback_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    items = load_items(input_path)
    grouped = group_by_store(items, fallback_date)

    for store_slug, store_items in grouped.items():
        output_path = input_path.parent / f"{store_slug}.json"
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(store_items, fh, ensure_ascii=False, indent=2)
            fh.write("\n")

    print(f"Split {len(items)} items into {len(grouped)} store files in {input_path.parent}.")


if __name__ == "__main__":
    main()
