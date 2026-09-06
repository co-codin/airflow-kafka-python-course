from pathlib import Path

from include.etl.regions import list_regions, write_region_slice
from include.features import build_customer_features, features_from_orders_csv
from include.paths import RAW_ORDERS


def test_build_customer_features_skips_cancelled():
    rows = [
        {
            "order_id": "ORD-1",
            "order_ts": "2026-01-02T00:00:00+00:00",
            "customer_id": "CUST-1",
            "customer_name": "Ada",
            "email": "ada@example.com",
            "region": "NA",
            "product": "Mouse",
            "category": "accessories",
            "quantity": "2",
            "unit_price": "10.00",
            "currency": "USD",
            "status": "paid",
        },
        {
            "order_id": "ORD-2",
            "order_ts": "2026-01-03T00:00:00+00:00",
            "customer_id": "CUST-1",
            "customer_name": "Ada",
            "email": "ada@example.com",
            "region": "EU",
            "product": "Hub",
            "category": "accessories",
            "quantity": "1",
            "unit_price": "5.00",
            "currency": "USD",
            "status": "cancelled",
        },
    ]
    features = build_customer_features(rows)
    assert len(features) == 1
    assert features[0]["order_count"] == "1"
    assert features[0]["monetary"] == "20.00"
    assert features[0]["regions"] == "NA"


def test_features_from_sample_writes_manifest(tmp_path: Path):
    result = features_from_orders_csv(RAW_ORDERS, tmp_path)
    assert result["row_count"] > 0
    assert Path(result["feature_path"]).is_file()
    assert Path(result["manifest_path"]).is_file()
    assert "customer_id" in Path(result["manifest_path"]).read_text(encoding="utf-8")


def test_list_regions_from_sample():
    regions = list_regions(RAW_ORDERS)
    assert regions == ["NA", "EU", "APAC", "LATAM"]


def test_write_region_slice(tmp_path: Path):
    result = write_region_slice(RAW_ORDERS, tmp_path / "eu.csv", "EU")
    assert result["region"] == "EU"
    assert result["row_count"] > 0
    text = Path(result["path"]).read_text(encoding="utf-8")
    assert "EU" in text
