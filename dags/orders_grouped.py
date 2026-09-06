"""Lesson DAG: the same CSV ETL wrapped in TaskGroups."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group

from include.etl.extract import extract_orders
from include.etl.load import load_orders_csv
from include.etl.transform import transform_orders
from include.etl.validate import assert_valid
from include.paths import PROCESSED_DIR, RAW_ORDERS, STAGING_DIR, ensure_dirs

DOC_MD = """
### orders_grouped

`TaskGroup` keeps a growing graph readable. Two groups:

* **ingest** — extract + transform
* **publish** — validate + CSV load

UI graph view nests tasks. This is the same logic as `orders_pipeline`
without DuckDB, so you can compare grouping vs a flat DAG.
"""


@dag(
    dag_id="orders_grouped",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "taskgroup"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_grouped():
    @task_group(group_id="ingest")
    def ingest() -> str:
        @task
        def extract() -> str:
            ensure_dirs()
            return extract_orders(RAW_ORDERS, STAGING_DIR / "grouped_extracted.csv")

        @task
        def transform(source_path: str) -> str:
            return transform_orders(source_path, STAGING_DIR / "grouped_transformed.csv")

        return transform(extract())

    @task_group(group_id="publish")
    def publish(source_path: str) -> str:
        @task
        def validate(path: str) -> dict:
            return assert_valid(path, min_rows=1)

        @task
        def load(path: str, _report: dict) -> str:
            return load_orders_csv(path, PROCESSED_DIR / "orders_grouped.csv")

        return load(source_path, validate(source_path))

    publish(ingest())


orders_grouped()
