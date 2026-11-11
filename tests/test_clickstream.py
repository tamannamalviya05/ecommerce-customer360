import json

import pytest

from src.ingestion.clickstream_consumer import (
    append_bronze_record,
    load_existing_event_ids,
    prepare_bronze_record,
)
from src.producers.clickstream_producer import generate_events


def test_generate_events_returns_requested_count():
    events = list(generate_events(100, seed=42))

    assert len(events) == 100


def test_generate_events_has_required_fields():
    event = next(generate_events(1, seed=42))

    required_fields = {
        "event_id",
        "event_timestamp",
        "customer_id",
        "session_id",
        "event_type",
        "page_url",
        "product_id",
        "referrer",
        "device_type",
    }

    assert required_fields.issubset(event.keys())
    assert event["customer_id"] >= 1
    assert event["event_type"] in {
        "page_view",
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "purchase",
    }


def test_generate_events_rejects_zero_count():
    with pytest.raises(ValueError, match="count must be at least 1"):
        list(generate_events(0))


def test_prepare_bronze_record_adds_metadata():
    event = {"event_id": "event-001", "event_type": "page_view"}

    record = prepare_bronze_record(
        event=event,
        topic="ecommerce.clickstream",
        partition=0,
        offset=12,
    )

    assert record["_source_system"] == "kafka_clickstream"
    assert record["_topic"] == "ecommerce.clickstream"
    assert record["_partition"] == 0
    assert record["_offset"] == 12
    assert record["_batch_id"] == "ecommerce.clickstream-0-12"
    assert record["_event_id"] == "event-001"
    assert record["raw_payload"] == event
    assert "_ingested_at" in record


def test_prepare_bronze_record_rejects_missing_event_id():
    with pytest.raises(ValueError, match="event_id"):
        prepare_bronze_record(
            event={"event_type": "page_view"},
            topic="ecommerce.clickstream",
            partition=0,
            offset=0,
        )


def test_append_bronze_record_and_skip_duplicate(tmp_path):
    record = {
        "_topic": "ecommerce.clickstream",
        "_partition": 0,
        "_offset": 1,
        "_batch_id": "ecommerce.clickstream-0-1",
        "_event_id": "event-001",
        "raw_payload": {"event_id": "event-001"},
    }
    seen_ids = set()

    output_path = append_bronze_record(record, tmp_path, seen_ids)
    append_bronze_record(record, tmp_path, seen_ids)

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    assert json.loads(lines[0])["_event_id"] == "event-001"
    assert seen_ids == {"event-001"}


def test_load_existing_event_ids(tmp_path):
    output_path = tmp_path / "clickstream_events.jsonl"
    records = [
        {"_event_id": "event-001"},
        {"_event_id": "event-002"},
    ]
    output_path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )

    assert load_existing_event_ids(tmp_path) == {"event-001", "event-002"}
