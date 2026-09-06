"""Lesson DAG: dynamic task mapping over order regions."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from include.etl.extract import extract_orders
from include.etl.regions import list_regions, write_region_slice
from include.paths import PROCESSED_DIR, RAW_ORDERS, STAGING_DIR, ensure_dirs

DOC_MD = """
### orders_mapped

Dynamic task mapping: one mapped task instance per **region** found in
the source CSV (`NA`, `EU`, `APAC`, `LATAM`).

1. extract the raw file
2. `list_regions` returns a list (XCom)
3. `process_region.expand(region=...)` fans out
4. `summarize` collects the mapped results

Add a new region to the generator and the map grows on the next run —
no DAG edit required. See `docs/LESSONS.md` (mapping).
"""


@dag(
    dag_id="orders_mapped",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "mapping"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_mapped():
    @task
    def extract() -> str:
        ensure_dirs()
        return extract_orders(RAW_ORDERS, STAGING_DIR / "mapped_extracted.csv")

    @task
    def regions(source_path: str) -> list[str]:
        found = list_regions(source_path)
        if not found:
            raise ValueError(f"no regions in {source_path}")
        return found

    @task
    def process_region(source_path: str, region: str) -> dict:
        dest = PROCESSED_DIR / f"orders_{region.lower()}.csv"
        return write_region_slice(source_path, dest, region)

    @task
    def summarize(slices: list[dict]) -> dict:
        return {
            "regions": [item["region"] for item in slices],
            "row_count": sum(item["row_count"] for item in slices),
        }

    extracted = extract()
    summarize(process_region.partial(source_path=extracted).expand(region=regions(extracted)))


orders_mapped()
