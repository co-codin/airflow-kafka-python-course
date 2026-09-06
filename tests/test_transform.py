from decimal import Decimal

import pytest

from include.etl.transform import normalize_email, parse_money, parse_quantity, transform_row


def test_normalize_email_strips_and_lowers():
    assert normalize_email("  Ada.Lovelace@Example.COM ") == "ada.lovelace@example.com"


def test_parse_money_rejects_negative():
    with pytest.raises(ValueError, match="unit_price"):
        parse_money("-1.00")


def test_parse_quantity_rejects_zero():
    with pytest.raises(ValueError, match="quantity"):
        parse_quantity("0")


def test_transform_row_adds_line_total():
    row = transform_row(
        {
            "order_id": "ORD-1",
            "order_ts": "2026-01-01T00:00:00+00:00",
            "customer_id": "CUST-1",
            "customer_name": "Ada Lovelace",
            "email": "Ada@Example.COM",
            "region": "eu",
            "product": "USB-C Hub",
            "category": "accessories",
            "quantity": "3",
            "unit_price": "10.00",
            "currency": "EUR",
            "status": "Paid",
        }
    )
    assert row["email"] == "ada@example.com"
    assert row["region"] == "EU"
    assert row["status"] == "paid"
    assert row["line_total"] == "30.00"
    assert row["is_cancelled"] == "false"
    assert Decimal(row["line_total"]) == Decimal("30.00")


def test_transform_row_flags_cancelled():
    row = transform_row(
        {
            "order_id": "ORD-2",
            "order_ts": "2026-01-01T00:00:00+00:00",
            "customer_id": "CUST-2",
            "customer_name": "Grace Hopper",
            "email": "grace@example.com",
            "region": "NA",
            "product": "Laptop Stand",
            "category": "accessories",
            "quantity": "1",
            "unit_price": "40.00",
            "currency": "USD",
            "status": "cancelled",
        }
    )
    assert row["is_cancelled"] == "true"
