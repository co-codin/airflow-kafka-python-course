# Airflow ↔ Kafka (Redpanda) patterns

This lab uses **Redpanda** as a Kafka-compatible broker (`localhost:9092` on the host, `redpanda:9092` inside Compose). Airflow talks to it through `apache-airflow-providers-apache-kafka` and the `kafka_default` connection.

## Connection

Compose pre-creates `kafka_default`:

```json
{
  "bootstrap.servers": "redpanda:9092",
  "security.protocol": "PLAINTEXT",
  "group.id": "airflow-course",
  "auto.offset.reset": "earliest"
}
```

In the UI: **Admin → Connections**. Extra / Config Dict is a JSON object of [librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) keys. `bootstrap.servers` is required.

Host-side scripts (the generator) use `localhost:9092` because port `19092` in the container is published as `9092`.

## Pattern 1 — Produce

`ProduceToTopicOperator` takes a **producer function** that yields `(key, value)` pairs:

```python
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

ProduceToTopicOperator(
    task_id="produce_orders",
    kafka_config_id="kafka_default",
    topic="orders",
    producer_function="include.kafka_io.produce_order_pairs",
    producer_function_kwargs={"source_path": "/opt/airflow/data/raw/orders.csv", "limit": 25},
    synchronous=True,
)
```

Keep the callable in `include/` (or a string import path). The operator imports it on the worker.

Also valid: `python scripts/generate_fake_orders.py --kafka --bootstrap localhost:9092`.

## Pattern 2 — Sensor

`AwaitMessageSensor` (provider) waits until a callable returns a truthy value for a message. Use it when a *downstream batch job* should start only after a specific event (for example a `load.complete` marker).

```python
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
```

Prefer `mode="reschedule"` so the slot is released between polls. This course's file lesson uses `FileSensor` for the same idea on disk.

## Pattern 3 — Bounded consume

`ConsumeFromTopicOperator` reads until **end of log** or **`max_messages`**, then stops:

```python
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

ConsumeFromTopicOperator(
    task_id="consume_orders",
    kafka_config_id="kafka_default",
    topics=["orders"],
    apply_function_batch="include.kafka_io.land_message_batch",
    apply_function_kwargs={"output_path": "/opt/airflow/data/processed/orders_kafka.jsonl"},
    max_messages=25,
    poll_timeout=20,
    commit_cadence="end_of_operator",
)
```

That is a **batch window**. Airflow is a scheduler, not a stream processor.

`apply_function` / `apply_function_batch` return values do **not** automatically become XCom. Land to a file (this lab) or push XCom yourself if you pass `ti` in kwargs.

## What NOT to do

| Anti-pattern | Why it hurts |
| --- | --- |
| Infinite `consumer.poll()` in a Python task | Ties up a worker forever; kills scheduling |
| Scheduling a DAG every second to mimic streaming | You will fight lag, overlapping runs, and cost |
| Sharing one `group.id` across unrelated DAGs | They steal each other's partitions |
| `max_messages=None` on a busy topic | The task can run until it times out or OOMs |
| Treating consume as exactly-once load | Commits and warehouse writes are two systems — use idempotent loads + offsets as a cursor |
| Putting secrets in DAG code | Use Connections / a secret backend |
| Producing giant payloads through XCom | XCom is for pointers (paths, keys), not the event firehose |

## Offsets and reruns

- `auto.offset.reset=earliest` helps the first lab run see data you just produced.
- A second consume with the same `group.id` will only see *new* messages unless you reset the group or change the id.
- For a teaching rerun, delete the consumer group (`rpk group delete airflow-course`) or bump `group.id`.
- Warehouse writes in this course use `CREATE OR REPLACE` / file overwrite so a replay does not duplicate rows.

## Mental model

```text
producer  -->  topic (orders)  -->  bounded consume  -->  JSONL / DuckDB
                 Redpanda              Airflow task
```

Use Kafka when multiple systems must share a log. Use files + sensors when the contract is "a drop landed in a bucket." Do not force every CSV through Kafka because it looks more "real-time."
