"""Lesson DAG: feature TaskGroup plus a versioned-style manifest."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task, task_group

from include.etl.extract import extract_orders
from include.etl.transform import transform_orders
from include.etl.validate import assert_valid
from include.features import features_from_orders_csv
from include.paths import PROCESSED_DIR, RAW_ORDERS, STAGING_DIR, ensure_dirs

DOC_MD = """
### feature_pipeline

A small "ML-ish" feature job on the same orders file:

1. **ingest** TaskGroup — extract, transform, validate
2. **features** TaskGroup — customer aggregates (counts, monetary, last ts)
3. Write `data/processed/customer_features.csv` and `feature_manifest.json`

The manifest records feature names, grain, and row count so a training
job can refuse to run against a silent schema change. Not a real feature
store — just the habit of shipping a contract with the table.
"""


@dag(
    dag_id="feature_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "features", "taskgroup"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def feature_pipeline():
    @task_group(group_id="ingest")
    def ingest() -> str:
        @task
        def extract() -> str:
            ensure_dirs()
            return extract_orders(RAW_ORDERS, STAGING_DIR / "feature_extracted.csv")

        @task
        def transform(source_path: str) -> str:
            return transform_orders(source_path, STAGING_DIR / "feature_transformed.csv")

        @task
        def validate(source_path: str) -> str:
            assert_valid(source_path, min_rows=1)
            return source_path

        return validate(transform(extract()))

    @task_group(group_id="features")
    def features(source_path: str) -> dict:
        @task
        def build(path: str) -> dict:
            return features_from_orders_csv(path, PROCESSED_DIR)

        return build(source_path)

    features(ingest())


feature_pipeline()
