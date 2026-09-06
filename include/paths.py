"""Filesystem layout used by scripts, DAGs, and tests."""

from __future__ import annotations

import os
from pathlib import Path

DATA_ROOT = Path(os.environ.get("COURSE_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))

RAW_DIR = DATA_ROOT / "raw"
INCOMING_DIR = DATA_ROOT / "incoming"
STAGING_DIR = DATA_ROOT / "staging"
PROCESSED_DIR = DATA_ROOT / "processed"
WAREHOUSE_DIR = DATA_ROOT / "warehouse"

RAW_ORDERS = RAW_DIR / "orders.csv"
INCOMING_ORDERS = INCOMING_DIR / "orders.csv"
DUCKDB_PATH = WAREHOUSE_DIR / "orders.duckdb"
FEATURE_MANIFEST = PROCESSED_DIR / "feature_manifest.json"
KAFKA_JSONL = PROCESSED_DIR / "orders_kafka.jsonl"

REGIONS = ("NA", "EU", "APAC", "LATAM")
STATUSES = ("pending", "paid", "shipped", "cancelled", "refunded")
CURRENCIES = ("USD", "EUR", "GBP")

ORDER_COLUMNS = (
    "order_id",
    "order_ts",
    "customer_id",
    "customer_name",
    "email",
    "region",
    "product",
    "category",
    "quantity",
    "unit_price",
    "currency",
    "status",
)


def ensure_dirs() -> None:
    for directory in (RAW_DIR, INCOMING_DIR, STAGING_DIR, PROCESSED_DIR, WAREHOUSE_DIR):
        directory.mkdir(parents=True, exist_ok=True)
