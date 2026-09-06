#!/usr/bin/env python3
"""Generate fake e-commerce orders as CSV and/or Kafka JSON events.

Examples
--------
    python scripts/generate_fake_orders.py --rows 80 --output data/raw/orders.csv
    python scripts/generate_fake_orders.py --rows 20 --incoming
    python scripts/generate_fake_orders.py --rows 10 --jsonl data/raw/events.jsonl
    python scripts/generate_fake_orders.py --rows 15 --kafka --bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow `python scripts/generate_fake_orders.py` from the repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from include.paths import (  # noqa: E402
    INCOMING_ORDERS,
    ORDER_COLUMNS,
    RAW_ORDERS,
    REGIONS,
    STATUSES,
    ensure_dirs,
)

PRODUCTS = (
    ("Wireless Mouse", "accessories"),
    ("USB-C Hub", "accessories"),
    ("Laptop Stand", "accessories"),
    ("Mechanical Keyboard", "peripherals"),
    ("27in Monitor", "displays"),
    ("Noise-cancelling Headphones", "audio"),
    ("USB Microphone", "audio"),
    ("Webcam HD", "peripherals"),
    ("Office Chair", "furniture"),
    ("Standing Desk", "furniture"),
)

FIRST_NAMES = (
    "Ava", "Noah", "Mia", "Liam", "Zoe", "Eli", "Nora", "Kai",
    "Ivy", "Owen", "Luna", "Theo", "Aria", "Leo", "Chloe", "Hugo",
)
LAST_NAMES = (
    "Nguyen", "Patel", "Garcia", "Kim", "Silva", "Andersson",
    "Okoye", "Rossi", "Berg", "Costa", "Nakamura", "Singh",
)

try:
    from faker import Faker

    _FAKER: Faker | None = Faker()
except ImportError:
    _FAKER = None


def _name(rng: random.Random) -> str:
    if _FAKER is not None:
        return _FAKER.name()
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _email(name: str, rng: random.Random) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "." for ch in name).strip(".")
    slug = ".".join(part for part in slug.split(".") if part)
    domain = rng.choice(("example.com", "shop.test", "mail.dev"))
    return f"{slug}@{domain}"


def make_order(index: int, rng: random.Random, now: datetime) -> dict[str, str]:
    name = _name(rng)
    product, category = rng.choice(PRODUCTS)
    quantity = rng.randint(1, 5)
    unit_price = round(rng.uniform(9.99, 499.0), 2)
    offset_hours = rng.randint(0, 24 * 45)
    order_ts = now - timedelta(hours=offset_hours, minutes=rng.randint(0, 59))
    return {
        "order_id": f"ORD-{index:06d}",
        "order_ts": order_ts.replace(microsecond=0).isoformat(),
        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
        "customer_name": name,
        "email": _email(name, rng),
        "region": rng.choice(REGIONS),
        "product": product,
        "category": category,
        "quantity": str(quantity),
        "unit_price": f"{unit_price:.2f}",
        "currency": rng.choice(("USD", "EUR", "GBP")),
        "status": rng.choices(
            STATUSES,
            weights=(15, 45, 25, 10, 5),
            k=1,
        )[0],
    }


def generate_orders(rows: int, seed: int) -> list[dict[str, str]]:
    rng = random.Random(seed)
    if _FAKER is not None:
        _FAKER.seed_instance(seed)
    now = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    return [make_order(i + 1, rng, now) for i in range(rows)]


def write_csv(path: Path, orders: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ORDER_COLUMNS))
        writer.writeheader()
        writer.writerows(orders)


def write_jsonl(path: Path, orders: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for order in orders:
            handle.write(json.dumps(order_to_event(order), separators=(",", ":")) + "\n")


def order_to_event(order: dict[str, str]) -> dict:
    """Kafka-friendly payload (numbers as numbers)."""
    return {
        **order,
        "quantity": int(order["quantity"]),
        "unit_price": float(order["unit_price"]),
        "event_type": "order.created",
    }


def produce_kafka(orders: list[dict[str, str]], bootstrap: str, topic: str) -> int:
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise SystemExit(
            "confluent-kafka is required for --kafka. "
            "Install it or omit --kafka."
        ) from exc

    producer = Producer({"bootstrap.servers": bootstrap})
    for order in orders:
        event = order_to_event(order)
        producer.produce(
            topic,
            key=event["order_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
    producer.flush(10)
    return len(orders)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=80, help="Number of orders to generate")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for repeatable files")
    parser.add_argument(
        "--output",
        type=Path,
        default=RAW_ORDERS,
        help="CSV destination (default: data/raw/orders.csv)",
    )
    parser.add_argument(
        "--incoming",
        action="store_true",
        help=f"Also write a copy to {INCOMING_ORDERS} for the FileSensor DAG",
    )
    parser.add_argument("--jsonl", type=Path, help="Optional JSONL of Kafka-shaped events")
    parser.add_argument("--kafka", action="store_true", help="Produce events to Kafka/Redpanda")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="orders", help="Kafka topic")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ensure_dirs()
    orders = generate_orders(args.rows, args.seed)
    write_csv(args.output, orders)
    print(f"Wrote {len(orders)} orders to {args.output}")
    if args.incoming:
        write_csv(INCOMING_ORDERS, orders)
        print(f"Wrote incoming copy to {INCOMING_ORDERS}")
    if args.jsonl:
        write_jsonl(args.jsonl, orders)
        print(f"Wrote JSONL events to {args.jsonl}")
    if args.kafka:
        count = produce_kafka(orders, args.bootstrap, args.topic)
        print(f"Produced {count} events to {args.topic} @ {args.bootstrap}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
