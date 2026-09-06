"""Clean and enrich order rows. Pure functions — no I/O framework imports."""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.paths import REGIONS, STATUSES

CANCELLED_STATUSES = frozenset({"cancelled", "refunded"})


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def parse_money(value: str) -> Decimal:
    try:
        amount = Decimal(str(value).strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"Invalid unit_price: {value!r}") from exc
    if amount < 0:
        raise ValueError(f"unit_price must be >= 0, got {amount}")
    return amount.quantize(Decimal("0.01"))


def parse_quantity(value: str) -> int:
    quantity = int(str(value).strip())
    if quantity <= 0:
        raise ValueError(f"quantity must be > 0, got {quantity}")
    return quantity


def transform_row(row: dict[str, str]) -> dict[str, str]:
    """Normalize one order and add line_total / flags."""
    quantity = parse_quantity(row["quantity"])
    unit_price = parse_money(row["unit_price"])
    region = (row.get("region") or "").strip().upper()
    status = (row.get("status") or "").strip().lower()
    if region not in REGIONS:
        raise ValueError(f"Unknown region {region!r} for {row.get('order_id')}")
    if status not in STATUSES:
        raise ValueError(f"Unknown status {status!r} for {row.get('order_id')}")

    line_total = (unit_price * quantity).quantize(Decimal("0.01"))
    return {
        **row,
        "email": normalize_email(row.get("email", "")),
        "region": region,
        "status": status,
        "quantity": str(quantity),
        "unit_price": f"{unit_price:.2f}",
        "line_total": f"{line_total:.2f}",
        "is_cancelled": "true" if status in CANCELLED_STATUSES else "false",
    }


def transform_orders(source_path: Path | str, dest_path: Path | str) -> str:
    """Read a staged CSV, write an enriched CSV, return the output path."""
    rows = [transform_row(row) for row in read_orders_csv(source_path)]
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return str(dest)
