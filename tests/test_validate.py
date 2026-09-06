import csv
from pathlib import Path

import pytest

from include.etl.validate import ValidationError, assert_valid, validate_orders
from include.paths import ORDER_COLUMNS


def _write(path: Path, rows: list[dict[str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ORDER_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _row(**overrides: str) -> dict[str, str]:
    base = {
        "order_id": "ORD-1",
        "order_ts": "2026-01-01T00:00:00+00:00",
        "customer_id": "CUST-1",
        "customer_name": "Ada",
        "email": "ada@example.com",
        "region": "NA",
        "product": "Mouse",
        "category": "accessories",
        "quantity": "1",
        "unit_price": "9.99",
        "currency": "USD",
        "status": "paid",
    }
    base.update(overrides)
    return base


def test_validate_orders_ok(tmp_path: Path):
    path = _write(tmp_path / "ok.csv", [_row(), _row(order_id="ORD-2", region="EU")])
    report = validate_orders(path, min_rows=2)
    assert report.ok
    assert report.row_count == 2
    assert report.regions["EU"] == 1


def test_assert_valid_rejects_duplicate_ids(tmp_path: Path):
    path = _write(tmp_path / "dup.csv", [_row(), _row()])
    with pytest.raises(ValidationError, match="unique"):
        assert_valid(path)


def test_assert_valid_rejects_empty(tmp_path: Path):
    path = _write(tmp_path / "empty.csv", [])
    with pytest.raises(ValidationError, match="min_rows"):
        assert_valid(path, min_rows=1)
