"""Consume Debezium MySQL CDC events and persist raw Bronze JSONL records."""

from __future__ import annotations

import base64
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

TOPICS = (
    "ecommerce.ecommerce.customers",
    "ecommerce.ecommerce.products",
    "ecommerce.ecommerce.orders",
)

EventIdentity = tuple[str, int, int]


def decode_connect_decimal(value: str | bytes, scale: int = 2) -> str:
    """Decode a Kafka Connect Decimal value encoded as base64 bytes."""
    raw = base64.b64decode(value) if isinstance(value, str) else value
    unscaled = int.from_bytes(raw, byteorder="big", signed=True)
    return str(Decimal(unscaled).scaleb(-scale))


def normalize_decimal_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Decode known MySQL DECIMAL fields in a Debezium record."""
    normalized = dict(record)
    payload = normalized.get("payload")

    if not isinstance(payload, dict):
        return normalized

    for image in ("after", "before"):
        row = payload.get(image)
        if isinstance(row, dict):
            row = dict(row)
            for field in ("price", "total_amount"):
                value = row.get(field)
                if isinstance(value, str):
                    row[field] = decode_connect_decimal(value)
            payload[image] = row

    normalized["payload"] = payload
    return normalized


def prepare_bronze_record(
    event: dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
) -> dict[str, Any]:
    """Add ingestion metadata while preserving the original CDC envelope."""
    return {
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source_system": "mysql_debezium",
        "_topic": topic,
        "_partition": partition,
        "_offset": offset,
        "_batch_id": f"{topic}-{partition}-{offset}",
        "raw_payload": event,
    }


def load_existing_event_ids(output_dir: Path) -> set[EventIdentity]:
    """Load previously persisted Kafka event identities from Bronze JSONL."""
    existing_ids: set[EventIdentity] = set()

    if not output_dir.exists():
        return existing_ids

    for file_path in output_dir.glob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                    identity = (
                        str(record["_topic"]),
                        int(record["_partition"]),
                        int(record["_offset"]),
                    )
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Invalid Bronze record in {file_path}, line {line_number}"
                    ) from exc

                existing_ids.add(identity)

    return existing_ids


def append_bronze_record(
    record: dict[str, Any],
    output_dir: Path,
    seen_event_ids: set[EventIdentity] | None = None,
) -> Path:
    """Append an event once, using its Kafka identity for replay protection."""
    output_dir.mkdir(parents=True, exist_ok=True)
    topic = str(record["_topic"]).replace(".", "_")
    output_path = output_dir / f"{topic}.jsonl"

    identity = (
        str(record["_topic"]),
        int(record["_partition"]),
        int(record["_offset"]),
    )

    if seen_event_ids is not None and identity in seen_event_ids:
        LOGGER.info("Skipping previously stored CDC event: %s", record["_batch_id"])
        return output_path

    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    if seen_event_ids is not None:
        seen_event_ids.add(identity)

    return output_path


def consume(
    bootstrap_servers: str = "localhost:29092",
    output_dir: Path = Path("data/bronze/mysql_cdc"),
) -> None:
    """Consume CDC messages continuously and append them idempotently to Bronze."""
    from kafka import KafkaConsumer
    from kafka.structs import OffsetAndMetadata, TopicPartition

    seen_event_ids = load_existing_event_ids(output_dir)
    LOGGER.info("Loaded %s existing Bronze event identities", len(seen_event_ids))

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda value: json.loads(value.decode("utf-8")) if value else None,
        group_id="ecommerce-mysql-cdc-bronze",
    )

    LOGGER.info("Listening to MySQL CDC topics: %s", TOPICS)

    try:
        for message in consumer:
            event = message.value

            if not isinstance(event, dict):
                LOGGER.warning(
                    "Skipping non-object event at %s:%s",
                    message.topic,
                    message.offset,
                )
                continue

            event = normalize_decimal_fields(event)
            record = prepare_bronze_record(
                event=event,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )

            append_bronze_record(record, output_dir, seen_event_ids)

            # Commit only this partition's next offset, after Bronze persistence.
            topic_partition = TopicPartition(message.topic, message.partition)
            consumer.commit(offsets={topic_partition: OffsetAndMetadata(message.offset + 1, "")})

            LOGGER.info("Processed CDC event: %s", record["_batch_id"])

    except KeyboardInterrupt:
        LOGGER.info("CDC consumer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    consume()
