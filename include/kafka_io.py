"""Serialize order events for Kafka. No Airflow imports — operators call these."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

from include.etl.extract import read_orders_csv
from include.paths import KAFKA_JSONL, RAW_ORDERS, ensure_dirs


def order_to_event(order: dict[str, str]) -> dict:
    """Kafka-friendly payload (numbers as numbers)."""
    return {
        **order,
        "quantity": int(order["quantity"]),
        "unit_price": float(order["unit_price"]),
        "event_type": "order.created",
    }


def events_from_csv(source_path: Path | str, limit: int | None = None) -> list[dict]:
    rows = read_orders_csv(source_path)
    if limit is not None:
        rows = rows[:limit]
    return [order_to_event(row) for row in rows]


def produce_order_pairs(
    source_path: str = str(RAW_ORDERS),
    limit: int = 25,
) -> Iterator[tuple[str, str]]:
    """Yield (key, value) pairs for ProduceToTopicOperator."""
    for event in events_from_csv(source_path, limit=limit):
        yield event["order_id"], json.dumps(event, separators=(",", ":"))


def message_to_event(message) -> dict:
    raw = message.value() if hasattr(message, "value") else message
    if raw is None:
        raise ValueError("empty Kafka message")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, dict):
        return raw
    return json.loads(raw)


def land_jsonl(events: list[dict], dest_path: Path | str) -> str:
    ensure_dirs()
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    return str(dest)


def land_message_batch(messages, output_path: str = str(KAFKA_JSONL)) -> str:
    """ConsumeFromTopicOperator apply_function_batch callable."""
    events = [message_to_event(message) for message in messages]
    return land_jsonl(events, output_path)


def count_jsonl(path: Path | str) -> dict:
    file_path = Path(path)
    if not file_path.is_file():
        return {"path": str(file_path), "row_count": 0}
    row_count = sum(1 for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip())
    return {"path": str(file_path), "row_count": row_count}
