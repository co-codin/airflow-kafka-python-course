"""Lesson DAG: wait for a dropped file, then run the CSV ETL."""

from __future__ import annotations

import pendulum
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task

from include.etl.extract import extract_orders
from include.etl.load import load_orders_csv
from include.etl.transform import transform_orders
from include.paths import INCOMING_ORDERS, PROCESSED_DIR, STAGING_DIR, ensure_dirs

DOC_MD = """
### orders_with_sensor

`FileSensor` pokes `/opt/airflow/data/incoming/orders.csv` using the
`fs_default` connection (local filesystem).

Drop a file, then the rest of the DAG runs:

```bash
# from the repo root, with the stack up
python scripts/generate_fake_orders.py --rows 20 --incoming

# or copy the checked-in sample
cp data/raw/orders.csv data/incoming/orders.csv
```

The sensor uses `mode="reschedule"` so it frees the worker slot between pokes.
Poke interval is short for the lab; production sensors should wait longer.
"""


@dag(
    dag_id="orders_with_sensor",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "sensor", "files"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_with_sensor():
    wait_for_orders = FileSensor(
        task_id="wait_for_orders",
        filepath=str(INCOMING_ORDERS),
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=60 * 30,
        mode="reschedule",
    )

    @task
    def extract() -> str:
        ensure_dirs()
        return extract_orders(INCOMING_ORDERS, STAGING_DIR / "sensor_extracted.csv")

    @task
    def transform(source_path: str) -> str:
        return transform_orders(source_path, STAGING_DIR / "sensor_transformed.csv")

    @task
    def load(source_path: str) -> str:
        return load_orders_csv(source_path, PROCESSED_DIR / "orders_from_sensor.csv")

    extracted = extract()
    loaded = load(transform(extracted))
    wait_for_orders >> extracted
    loaded


orders_with_sensor()
