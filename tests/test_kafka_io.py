import json
from pathlib import Path

from include.kafka_io import (
    count_jsonl,
    events_from_csv,
    land_jsonl,
    land_message_batch,
    message_to_event,
    produce_order_pairs,
)
from include.paths import RAW_ORDERS


class _FakeMessage:
    def __init__(self, payload: dict):
        self._payload = json.dumps(payload).encode("utf-8")

    def value(self):
        return self._payload


def test_events_from_csv_limit():
    events = events_from_csv(RAW_ORDERS, limit=3)
    assert len(events) == 3
    assert events[0]["event_type"] == "order.created"
    assert isinstance(events[0]["quantity"], int)


def test_produce_order_pairs_are_json():
    pairs = list(produce_order_pairs(str(RAW_ORDERS), limit=2))
    assert len(pairs) == 2
    key, value = pairs[0]
    assert key.startswith("ORD-")
    payload = json.loads(value)
    assert payload["order_id"] == key


def test_land_message_batch_writes_jsonl(tmp_path: Path):
    dest = tmp_path / "out.jsonl"
    messages = [_FakeMessage({"order_id": "ORD-1"}), _FakeMessage({"order_id": "ORD-2"})]
    land_message_batch(messages, output_path=str(dest))
    lines = dest.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(line)["order_id"] for line in lines] == ["ORD-1", "ORD-2"]
    assert count_jsonl(dest)["row_count"] == 2


def test_message_to_event_accepts_bytes_and_dict():
    assert message_to_event(_FakeMessage({"a": 1})) == {"a": 1}
    assert message_to_event({"a": 2}) == {"a": 2}


def test_land_jsonl_roundtrip(tmp_path: Path):
    path = land_jsonl([{"order_id": "ORD-9"}], tmp_path / "x.jsonl")
    assert Path(path).read_text(encoding="utf-8").strip() == '{"order_id":"ORD-9"}'
