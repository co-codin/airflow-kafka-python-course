"""Lesson DAG: produce fake orders to Kafka, then bounded-consume to JSONL."""

from __future__ import annotations

import os

import pendulum
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.sdk import dag, task

from include.kafka_io import count_jsonl
from include.paths import KAFKA_JSONL, RAW_ORDERS, ensure_dirs

KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
MAX_MESSAGES = 25

DOC_MD = """
### orders_kafka

Uses the `apache.kafka` provider and the `kafka_default` connection
(wired in `docker-compose.yml` to Redpanda).

1. **produce_orders** — `ProduceToTopicOperator` + `include.kafka_io.produce_order_pairs`
2. **consume_orders** — `ConsumeFromTopicOperator` with `max_messages` (bounded)
3. **land** — write JSONL via `apply_function_batch`
4. **summarize** — count lines in `data/processed/orders_kafka.jsonl`

This is a **batch window** over a topic, not a streaming consumer. Do not
run an infinite poll inside an Airflow worker. See `docs/KAFKA.md`.
"""


@dag(
    dag_id="orders_kafka",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["lesson", "kafka", "redpanda"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    doc_md=DOC_MD,
)
def orders_kafka():
    @task
    def prepare() -> dict:
        ensure_dirs()
        if KAFKA_JSONL.exists():
            KAFKA_JSONL.unlink()
        return {"source_path": str(RAW_ORDERS), "topic": KAFKA_TOPIC, "limit": MAX_MESSAGES}

    produce_orders = ProduceToTopicOperator(
        task_id="produce_orders",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function="include.kafka_io.produce_order_pairs",
        producer_function_kwargs={
            "source_path": str(RAW_ORDERS),
            "limit": MAX_MESSAGES,
        },
        synchronous=True,
        poll_timeout=1,
    )

    consume_orders = ConsumeFromTopicOperator(
        task_id="consume_orders",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function_batch="include.kafka_io.land_message_batch",
        apply_function_kwargs={"output_path": str(KAFKA_JSONL)},
        max_messages=MAX_MESSAGES,
        max_batch_size=MAX_MESSAGES,
        poll_timeout=20,
        commit_cadence="end_of_operator",
    )

    @task
    def summarize() -> dict:
        return count_jsonl(KAFKA_JSONL)

    ready = prepare()
    ready >> produce_orders >> consume_orders >> summarize()


orders_kafka()
