from pathlib import Path

from include.etl.extract import extract_orders
from include.etl.transform import transform_orders
from include.etl.validate import assert_valid
from include.paths import RAW_ORDERS


def test_sample_raw_orders_pass_the_gate(tmp_path: Path):
    extracted = extract_orders(RAW_ORDERS, tmp_path / "extracted.csv")
    transformed = transform_orders(extracted, tmp_path / "transformed.csv")
    report = assert_valid(transformed, min_rows=10)
    assert report["row_count"] == 80
    assert report["unique_order_ids"] == 80
