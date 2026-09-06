"""Helpers for the dynamic-mapping lesson."""

from __future__ import annotations

import csv
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.etl.transform import transform_row
from include.paths import REGIONS


def list_regions(source_path: Path | str) -> list[str]:
    """Sorted regions present in the file (stable mapping order)."""
    found = {row["region"].strip().upper() for row in read_orders_csv(source_path)}
    return [region for region in REGIONS if region in found]


def write_region_slice(source_path: Path | str, dest_path: Path | str, region: str) -> dict:
    """Filter transformed rows to one region and write a CSV."""
    region = region.strip().upper()
    rows = [
        transform_row(row)
        for row in read_orders_csv(source_path)
        if row.get("region", "").strip().upper() == region
    ]
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else ["order_id"]
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return {"region": region, "path": str(dest), "row_count": len(rows)}
