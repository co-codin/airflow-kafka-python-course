# Airflow + Kafka + Python ETL course

Hands-on data engineering course: Apache Airflow 3, Python ETL, Redpanda (Kafka-compatible), DuckDB, Docker Compose, and fake order data.

This repo is a lab, not a slide deck. You run the stack, trigger DAGs, read the `include/` functions, and break things on purpose.

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

Business logic lives in `include/` as plain Python. DAGs in `dags/` only orchestrate.

## Quickstart

Stack and login details land in later commits (`docker-compose.yml`, `.env.example`). The intended flow:

```bash
cp .env.example .env
docker compose up --build
```

Then open the Airflow UI, log in, unpause a DAG, and trigger it.

## Repo layout

```text
dags/           # Airflow 3 TaskFlow DAGs (airflow.sdk)
include/        # Pure Python (no Airflow imports in transform)
scripts/        # Fake data generator
data/raw/       # Sample CSVs for offline runs
tests/          # pytest for include/
docs/           # Lessons + Kafka patterns
```

## Docs

- [docs/LESSONS.md](docs/LESSONS.md) — condensed curriculum
- [docs/KAFKA.md](docs/KAFKA.md) — Airflow ↔ Kafka patterns

## License

Course materials in this repository are provided for learning and portfolio use.
