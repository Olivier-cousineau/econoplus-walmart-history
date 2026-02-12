#!/usr/bin/env python3
"""Tag snapshot items with store metadata when missing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_items(path: Path) -> tuple[list[dict[str, Any]], Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)], data

    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return [item for item in data["items"] if isinstance(item, dict)], data

    raise ValueError(f"Unsupported JSON format in {path}")


def snapshot_has_store_slug(items: list[dict[str, Any]]) -> bool:
    return any(isinstance(item.get("store_slug"), str) and item["store_slug"].strip() for item in items)


def tag_items(
    items: list[dict[str, Any]],
    store_slug: str,
    store_name: str,
    city: str,
    province: str,
) -> int:
    updated = 0
    for item in items:
        if not isinstance(item, dict):
            continue

        item["store_slug"] = store_slug
        item["store_name"] = store_name
        item["city"] = city
        item["province"] = province
        updated += 1

    return updated


def write_output(path: Path, raw_data: Any, items: list[dict[str, Any]]) -> None:
    if isinstance(raw_data, list):
        payload = items
    else:
        payload = dict(raw_data)
        payload["items"] = items

    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tag a Walmart snapshot with store metadata.")
    parser.add_argument("--file", required=True, help="Path to snapshot JSON file.")
    parser.add_argument("--store-slug", required=True)
    parser.add_argument("--store-name", required=True)
    parser.add_argument("--city", required=True)
    parser.add_argument("--province", required=True)
    parser.add_argument(
        "--if-missing-only",
        action="store_true",
        help="Only tag items when no item already has store_slug.",
    )
    args = parser.parse_args()

    snapshot_path = Path(args.file)
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Snapshot file not found: {snapshot_path}")

    items, raw_data = load_items(snapshot_path)

    if args.if_missing_only and snapshot_has_store_slug(items):
        print(f"Skipping {snapshot_path}: store_slug already present.")
        return

    updated = tag_items(
        items=items,
        store_slug=args.store_slug,
        store_name=args.store_name,
        city=args.city,
        province=args.province,
    )
    write_output(snapshot_path, raw_data, items)
    print(f"Tagged {updated} items in {snapshot_path}.")


if __name__ == "__main__":
    main()
