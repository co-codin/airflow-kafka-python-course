"""Lesson DAG: path XCom, validate gate, DuckDB load."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.etl.extract import extract_orders
from include.etl.load import load_orders_csv, load_orders_duckdb
from include.etl.transform import transform_orders
from include.etl.validate import assert_valid
from include.paths import DUCKDB_PATH, PROCESSED_DIR, RAW_ORDERS, STAGING_DIR, ensure_dirs

DOC_MD = """
### orders_pipeline

Same CSV path-passing as `orders_etl`, plus a **validate gate** and a
**DuckDB** load.

* Tasks exchange paths (strings) through XCom — not dataframes.
* `include.etl.validate.assert_valid` raises if the quality gate fails,
  so the warehouse load never sees bad data.
* DuckDB file: `data/warehouse/orders.duckdb` (table `orders`).
  `CREATE OR REPLACE TABLE` keeps reruns idempotent.

Run the same logic outside Airflow:

```bash
pytest tests/test_transform.py tests/test_validate.py tests/test_load.py
```
"""


@dag(
    dag_id="orders_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "duckdb", "quality"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_pipeline():
    @task
    def extract() -> str:
        ensure_dirs()
        return extract_orders(RAW_ORDERS, STAGING_DIR / "pipeline_extracted.csv")

    @task
    def transform(source_path: str) -> str:
        return transform_orders(source_path, STAGING_DIR / "pipeline_transformed.csv")

    @task
    def validate(source_path: str) -> dict:
        return assert_valid(source_path, min_rows=1)

    @task
    def load_csv(source_path: str, _report: dict) -> str:
        return load_orders_csv(source_path, PROCESSED_DIR / "orders_pipeline.csv")

    @task
    def load_duckdb(source_path: str, _report: dict) -> dict:
        return load_orders_duckdb(source_path, DUCKDB_PATH, table="orders")

    extracted = extract()
    transformed = transform(extracted)
    report = validate(transformed)
    load_csv(transformed, report)
    load_duckdb(transformed, report)


orders_pipeline()
