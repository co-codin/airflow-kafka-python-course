import csv
from pathlib import Path

import pytest

from include.etl.load import load_orders_csv
from include.paths import ORDER_COLUMNS


def test_load_orders_csv_overwrites(tmp_path: Path):
    source = tmp_path / "in.csv"
    dest = tmp_path / "out.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ORDER_COLUMNS))
        writer.writeheader()
        writer.writerow(
            {
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
        )
    assert load_orders_csv(source, dest) == str(dest)
    assert dest.is_file()
    assert dest.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_load_orders_duckdb_roundtrip(tmp_path: Path):
    duckdb = pytest.importorskip("duckdb")
    from include.etl.load import load_orders_duckdb

    source = tmp_path / "in.csv"
    source.write_text(
        "order_id,order_ts,customer_id,customer_name,email,region,product,"
        "category,quantity,unit_price,currency,status\n"
        "ORD-1,2026-01-01T00:00:00+00:00,CUST-1,Ada,ada@example.com,NA,Mouse,"
        "accessories,2,10.00,USD,paid\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "orders.duckdb"
    result = load_orders_duckdb(source, db_path)
    assert result["row_count"] == 1
    with duckdb.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
        # Idempotent replace, not append.
        load_orders_duckdb(source, db_path)
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
