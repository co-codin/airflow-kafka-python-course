# Lessons 1–32 (condensed)

Work through the DAGs in `dags/` while you read this. Each block is a habit, not a lecture.

## 1. ETL

Extract copies or queries a source. Transform cleans and shapes. Load writes a destination. In this repo those are **functions** (`include/etl/`), not Airflow objects. If you cannot run transform in pytest, it is not isolated enough.

## 2. Orchestration vs compute

Airflow decides *when* and *in what order*. DuckDB, Python, and Kafka do the work. If a task is a 40-line SQL script plus retries, that is orchestration. If it is a two-hour Spark job inlined in the DAG file, you mixed layers.

## 3. Idempotency

A rerun must not double-count. Patterns used here: overwrite CSV, `CREATE OR REPLACE TABLE` in DuckDB, bounded Kafka consume into a replaced JSONL. Avoid `INSERT` without a unique key or partition replace.

## 4. DAG

A DAG is a directed acyclic graph of tasks. Cycles are bugs. Airflow 3 TaskFlow: `from airflow.sdk import dag, task`. Call the decorated function at module bottom so the scheduler can find it.

## 5. Schedule and catchup

`schedule=None` means manual trigger (all lesson DAGs). `@daily` plus `catchup=True` will backfill every interval since `start_date` — surprise cluster bills. Set `catchup=False` until you *mean* to backfill. `start_date` is data time, not "when I deployed."

## 6. TaskFlow

Decorated functions return values; Airflow stores them as XCom and wires dependencies from the call graph. Prefer this over manual `PythonOperator` + `ti.xcom_pull` for new code.

## 7. XCom

XCom is a **small** inter-task bus (paths, counts, report dicts). This course passes file paths, not dataframes. Huge XComs blow up the metadata DB. The "path XCom" pattern is `orders_pipeline`.

## 8. Retries and timeouts

`retries=1` on lesson DAGs. Retry *transient* I/O, not bad data (that is a quality gate). Always set `timeout` on sensors. A sensor without a timeout is a zombie.

## 9. Connections and Variables

Connection = how to reach a system (`kafka_default`, `fs_default`, Postgres). Variable = a config knob. Never commit passwords. Compose injects `AIRFLOW_CONN_*` for the lab only.

## 10. Sensors

A sensor waits for a condition, then the graph continues. `orders_with_sensor` uses `FileSensor` on `data/incoming/orders.csv` in `reschedule` mode (release the slot between pokes). `poke` mode holds a worker the whole time — fine for a 15s lab, bad for a 3-hour drop window.

## 11. Dynamic task mapping

`process_region.partial(source_path=path).expand(region=regions)` builds one task per region at runtime. Mapped lists should be bounded and cheap to compute. See `orders_mapped`.

## 12. TaskGroups

`@task_group` nests ingest / publish / features so the UI stays readable. Same tasks, better map. See `orders_grouped` and `feature_pipeline`.

## 13. Assets (data-aware scheduling)

Airflow 3 assets (formerly Datasets) let DAG B start when DAG A updates an asset, instead of guessing with a time schedule. Use them when the trigger is "the table changed," not "it's 03:00."

## 14. Data quality gates

`assert_valid` fails the task if duplicates, blanks, or bad types appear. Fail **before** load. A red task is cheaper than a silent bad warehouse.

## 15. Testing

`pytest tests/` runs without Airflow. That is the point of `include/`. Test transform, validate, features, and Kafka serializers. Do not wait for a 5-minute scheduler loop to find a typo in `line_total`.

## 16. Fake data

`scripts/generate_fake_orders.py` builds repeatable CSVs (`--seed`) and optional Kafka events. Check in a small `data/raw/` fixture so CI and laptops work offline.

## 17. File layout

`raw` → `incoming` / `staging` → `processed` → `warehouse`. Raw is immutable input. Staging is disposable. Processed is the contract. Warehouse is queryable.

## 18. DuckDB as a pocket warehouse

File-based, no extra container. Good for labs, local analytics, and CI. Not a substitute for a shared warehouse when ten tools need concurrency and auth. `load_orders_duckdb` uses `read_csv_auto`.

## 19. Kafka produce

Yield `(key, value)` from a plain function. Key by `order_id` so the same order lands on the same partition. See `docs/KAFKA.md`.

## 20. Bounded consume

`max_messages` + `poll_timeout` make consume a batch. Airflow should not host a 24/7 consumer loop.

## 21. Streaming vs batch

Streaming (Flink, consumer groups in an app) is for low-latency reactions. Batch (this course) is for facts that can land every N minutes. Most "real-time" dashboards are fine at 5–15 minutes.

## 22. Lakehouse sketch

Bronze = raw JSONL/CSV. Silver = validated orders. Gold = features / aggregates. DuckDB or a table format (Iceberg/Delta) can sit on files. Kafka is the bus in; the lake is the memory.

## 23. Partitioning and backfills

Slice by `ds` / region / hour so you can replace one partition. Mapping by region is the toy version. Backfills replay a date range with the *same* idempotent load.

## 24. Schema evolution

Add columns as nullable; do not silently rename. The feature manifest (`feature_pipeline`) is a cheap contract: names, grain, row count. A trainer should refuse a mystery schema.

## 25. Feature pipelines

Aggregate to the grain you will train on (`customer_id` here). Drop cancelled orders if they are not a label. Ship a manifest next to the table.

## 26. Security

Local compose uses `airflow/airflow` and plaintext Kafka. Production: TLS, SASL, secret backend, least-privilege connections, no UI on the public internet. Fernet key and JWT secret must be unique and stored as secrets.

## 27. Secrets

`.env` is gitignored. `.env.example` has dummy values only. Rotate anything that ever leaked. DAG code should read `os.environ` or Connections, never a pasted token.

## 28. Cost

Idle Compose is a laptop tax; idle Celery clusters in the cloud are a bill. Sensors in poke mode waste CPU. Catchup over years of hourly DAGs can explode task count. Kafka retention is disk. Prefer LocalExecutor for this lab.

## 29. Observability

Read task logs first. Then XCom, then the warehouse row count. Add a summarize task that records `row_count` (see `orders_kafka`, `orders_mapped`). You cannot debug what you did not measure.

## 30. Docker Compose local

`docker compose up --build` starts API server (UI), scheduler, DAG processor, triggerer, Postgres, Redpanda. UI: `http://localhost:8080`. This is not production HA.

## 31. Airflow 3 shape

The UI is the **API server** (not `webserver`). DAG parsing is a **dag-processor**. Task execution talks to an execution API. TaskFlow public imports live in `airflow.sdk`.

## 32. After the lab

Move secrets to a backend, pin image digests, drop LocalExecutor for Celery/Kubernetes when you need isolation, put DuckDB behind a real warehouse if the data is shared, and keep `include/` as the portable core of the pipeline.
