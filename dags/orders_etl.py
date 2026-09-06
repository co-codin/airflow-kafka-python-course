"""Lesson DAG: extract → transform → load a CSV of fake orders."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.etl.extract import extract_orders
from include.etl.load import load_orders_csv
from include.etl.transform import transform_orders
from include.paths import PROCESSED_DIR, RAW_ORDERS, STAGING_DIR, ensure_dirs

DOC_MD = """
### orders_etl

First pipeline in the course. Three TaskFlow tasks pass **file paths** via XCom,
not the rows themselves.

1. **extract** — copy `data/raw/orders.csv` to staging (header check included)
2. **transform** — normalize email/region/status, compute `line_total`
3. **load** — write the cleaned CSV under `data/processed/`

Business logic lives in `include/etl/` so you can run it with pytest and
without starting Airflow. See `docs/LESSONS.md`.
"""


@dag(
    dag_id="orders_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "etl", "csv"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_etl():
    @task
    def extract() -> str:
        ensure_dirs()
        return extract_orders(RAW_ORDERS, STAGING_DIR / "orders_extracted.csv")

    @task
    def transform(source_path: str) -> str:
        return transform_orders(source_path, STAGING_DIR / "orders_transformed.csv")

    @task
    def load(source_path: str) -> str:
        return load_orders_csv(source_path, PROCESSED_DIR / "orders.csv")

    load(transform(extract()))


orders_etl()
