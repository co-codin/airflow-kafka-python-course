# Airflow + Kafka + Python ETL course

Hands-on data engineering lab: **Apache Airflow 3.3**, Python ETL, **Redpanda** (Kafka-compatible), **DuckDB**, Docker Compose, and fake order data.

You run the stack, trigger DAGs, read `include/`, and break things on purpose. DAGs only orchestrate. Business logic is plain Python.

## Quickstart

```bash
cp .env.example .env
docker compose up --build
```

Wait until `airflow-apiserver` is healthy, then open **http://localhost:8080**.

| | |
| --- | --- |
| Username | `airflow` |
| Password | `airflow` |
| Kafka (host) | `localhost:9092` |
| Kafka (in Compose) | `redpanda:9092` |
| DuckDB file | `data/warehouse/orders.duckdb` |

DAGs start **paused**. In the UI: unpause → Trigger.

Suggested order:

1. `orders_etl` — works offline against `data/raw/orders.csv`
2. `orders_pipeline` — validate gate + DuckDB
3. `orders_with_sensor` — drop a file first (see below)
4. `orders_grouped` / `orders_mapped` / `feature_pipeline`
5. `orders_kafka` — needs Redpanda healthy

```bash
# FileSensor drop
python scripts/generate_fake_orders.py --rows 20 --incoming
# or: cp data/raw/orders.csv data/incoming/orders.csv

# Optional: produce from the host
python scripts/generate_fake_orders.py --rows 15 --kafka --bootstrap localhost:9092
```

Stop the stack with `docker compose down`. Add `-v` only if you want to wipe the Airflow metadata volume.

## What you will build

| DAG | Lesson |
| --- | --- |
| `orders_etl` | Extract → transform → load on CSV |
| `orders_with_sensor` | `FileSensor` waits, then the pipeline runs |
| `orders_pipeline` | Path XCom, validate gate, DuckDB, pytest |
| `orders_mapped` | Dynamic task mapping by region |
| `orders_grouped` | `TaskGroup` |
| `feature_pipeline` | Feature `TaskGroup` + manifest |
| `orders_kafka` | Produce → bounded consume → JSONL |

## Tests (no Airflow required)

```bash
pip install -r requirements.txt   # or: pip install pytest duckdb
pytest
```

`include/etl/transform.py` and friends must stay free of `airflow` imports.

## Repo layout

```text
dags/              Airflow 3 TaskFlow DAGs (airflow.sdk)
include/           Pure Python (ETL, features, Kafka serializers)
scripts/           Fake data generator
data/raw/          Sample CSV + JSONL for offline runs
data/incoming/     FileSensor drop zone
data/warehouse/    DuckDB file (created at runtime)
tests/             pytest
docs/              Lessons + Kafka patterns
docker-compose.yml Airflow 3 (LocalExecutor) + Postgres + Redpanda
```

## Stack notes

- Image: `apache/airflow:3.3.1` plus `Dockerfile` extras (Kafka provider, DuckDB, Faker).
- Executor: **LocalExecutor** — no Redis/Celery. Official 3.x still needs API server, scheduler, DAG processor, triggerer, and Postgres.
- DuckDB is **file-based**; there is no DuckDB container.
- Redpanda advertises `redpanda:9092` inside the network and `localhost:9092` on the host (`9092:19092`).
- Login, JWT, and Kafka extras in `.env.example` are **local-dev dummies**. Do not reuse them.

First Linux run: set `AIRFLOW_UID=$(id -u)` in `.env` so mounted logs are writable.

If DAGs do not appear, check `airflow-dag-processor` logs and that `PYTHONPATH=/opt/airflow` is set (it is, in Compose).

## Docs

- [docs/LESSONS.md](docs/LESSONS.md) — condensed lessons 1–32
- [docs/KAFKA.md](docs/KAFKA.md) — produce, sensor, bounded consume, what not to do

## License

Course materials in this repository are provided for learning and portfolio use.
