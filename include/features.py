"""Lightweight feature tables for the ML-flavored lesson (RFM-style)."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.etl.transform import transform_row
from include.paths import FEATURE_MANIFEST, PROCESSED_DIR, ensure_dirs

FEATURE_COLUMNS = (
    "customer_id",
    "order_count",
    "paid_order_count",
    "quantity_sum",
    "monetary",
    "avg_order_value",
    "last_order_ts",
    "regions",
)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_customer_features(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Aggregate transformed orders into one feature row per customer."""
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        cleaned = transform_row(row) if "line_total" not in row else row
        if cleaned["is_cancelled"] == "true":
            continue
        buckets[cleaned["customer_id"]].append(cleaned)

    features: list[dict[str, str]] = []
    for customer_id, orders in sorted(buckets.items()):
        monetary = sum(Decimal(order["line_total"]) for order in orders)
        quantity_sum = sum(int(order["quantity"]) for order in orders)
        paid = sum(1 for order in orders if order["status"] in {"paid", "shipped"})
        last_ts = max(_parse_ts(order["order_ts"]) for order in orders)
        regions = sorted({order["region"] for order in orders})
        count = len(orders)
        avg = (monetary / count).quantize(Decimal("0.01")) if count else Decimal("0.00")
        features.append(
            {
                "customer_id": customer_id,
                "order_count": str(count),
                "paid_order_count": str(paid),
                "quantity_sum": str(quantity_sum),
                "monetary": f"{monetary:.2f}",
                "avg_order_value": f"{avg:.2f}",
                "last_order_ts": last_ts.isoformat(),
                "regions": "|".join(regions),
            }
        )
    return features


def write_features(rows: list[dict[str, str]], dest_path: Path | str) -> str:
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(FEATURE_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    return str(dest)


def write_feature_manifest(
    feature_path: Path | str,
    row_count: int,
    dest_path: Path | str | None = None,
) -> str:
    ensure_dirs()
    manifest_path = Path(dest_path) if dest_path else FEATURE_MANIFEST
    payload = {
        "feature_path": str(feature_path),
        "feature_names": list(FEATURE_COLUMNS),
        "row_count": row_count,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "grain": "customer_id",
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return str(manifest_path)


def features_from_orders_csv(source_path: Path | str, dest_dir: Path | str | None = None) -> dict:
    dest_root = Path(dest_dir) if dest_dir else PROCESSED_DIR
    rows = read_orders_csv(source_path)
    features = build_customer_features(rows)
    feature_path = write_features(features, dest_root / "customer_features.csv")
    manifest_path = write_feature_manifest(
        feature_path,
        row_count=len(features),
        dest_path=dest_root / "feature_manifest.json",
    )
    return {
        "feature_path": feature_path,
        "manifest_path": manifest_path,
        "row_count": len(features),
    }
