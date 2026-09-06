"""Quality gate for transformed orders. Raise on failure so the DAG stops."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.etl.transform import parse_money, parse_quantity
from include.paths import REGIONS, STATUSES


class ValidationError(ValueError):
    """One or more data-quality checks failed."""


@dataclass(frozen=True)
class ValidationReport:
    row_count: int
    unique_order_ids: int
    cancelled_count: int
    regions: dict[str, int]
    errors: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        return asdict(self)


def validate_orders(path: Path | str, min_rows: int = 1) -> ValidationReport:
    rows = read_orders_csv(path)
    errors: list[str] = []
    if len(rows) < min_rows:
        errors.append(f"row_count {len(rows)} is below min_rows={min_rows}")

    ids = [row.get("order_id", "") for row in rows]
    unique_ids = set(ids)
    if len(unique_ids) != len(ids):
        errors.append("order_id is not unique")
    if any(not order_id for order_id in ids):
        errors.append("order_id contains blanks")

    required = ("order_id", "customer_id", "email", "region", "quantity", "unit_price", "status")
    for index, row in enumerate(rows, start=2):
        for column in required:
            if not str(row.get(column, "")).strip():
                errors.append(f"line {index}: missing {column}")
        try:
            parse_quantity(row.get("quantity", ""))
        except ValueError as exc:
            errors.append(f"line {index}: {exc}")
        try:
            parse_money(row.get("unit_price", ""))
        except ValueError as exc:
            errors.append(f"line {index}: {exc}")
        region = (row.get("region") or "").strip().upper()
        if region and region not in REGIONS:
            errors.append(f"line {index}: unknown region {region}")
        status = (row.get("status") or "").strip().lower()
        if status and status not in STATUSES:
            errors.append(f"line {index}: unknown status {status}")

    cancelled = sum(
        1
        for row in rows
        if (row.get("status") or "").lower() in {"cancelled", "refunded"}
        or row.get("is_cancelled") == "true"
    )
    region_counts = Counter((row.get("region") or "").upper() for row in rows)
    return ValidationReport(
        row_count=len(rows),
        unique_order_ids=len(unique_ids),
        cancelled_count=cancelled,
        regions=dict(sorted(region_counts.items())),
        errors=errors[:50],
    )


def assert_valid(path: Path | str, min_rows: int = 1) -> dict:
    report = validate_orders(path, min_rows=min_rows)
    if not report.ok:
        raise ValidationError("quality gate failed: " + "; ".join(report.errors))
    return report.to_dict()
