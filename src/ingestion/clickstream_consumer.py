"""Consume Kafka clickstream events and persist raw Bronze JSONL records."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

LOGGER = logging.getLogger(__name__)

DEFAULT_TOPIC = "ecommerce.clickstream"
DEFAULT_OUTPUT_DIR = Path("data/bronze/clickstream")
DEFAULT_GROUP_ID = "ecommerce-clickstream-bronze"


def prepare_bronze_record(
    event: dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
) -> dict[str, Any]:
    """Add ingestion metadata while preserving the original clickstream event."""
    event_id = event.get("event_id")

    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("Clickstream event must contain a non-empty event_id")

    return {
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source_system": "kafka_clickstream",
        "_topic": topic,
        "_partition": partition,
        "_offset": offset,
        "_batch_id": f"{topic}-{partition}-{offset}",
        "_event_id": event_id,
        "raw_payload": event,
    }


def load_existing_event_ids(output_dir: Path) -> set[str]:
    """Load previously persisted clickstream event IDs from Bronze JSONL."""
    existing_ids: set[str] = set()

    if not output_dir.exists():
        return existing_ids

    for file_path in output_dir.glob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)
                    event_id = record["_event_id"]

                    if not isinstance(event_id, str) or not event_id.strip():
                        raise ValueError("Invalid event ID")

                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Invalid Bronze record in {file_path}, line {line_number}"
                    ) from exc

                existing_ids.add(event_id)

    return existing_ids


def append_bronze_record(
    record: dict[str, Any],
    output_dir: Path,
    seen_event_ids: set[str] | None = None,
) -> Path:
    """Append an event once, using its event ID for replay protection."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "clickstream_events.jsonl"
    event_id = str(record["_event_id"])

    if seen_event_ids is not None and event_id in seen_event_ids:
        LOGGER.info("Skipping previously stored clickstream event: %s", event_id)
        return output_path

    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    if seen_event_ids is not None:
        seen_event_ids.add(event_id)

    return output_path


def consume(
    bootstrap_servers: str = "localhost:29092",
    topic: str = DEFAULT_TOPIC,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int | None = None,
) -> int:
    """Consume clickstream messages and append them idempotently to Bronze."""
    if max_messages is not None and max_messages < 1:
        raise ValueError("max_messages must be at least 1")

    seen_event_ids = load_existing_event_ids(output_dir)
    LOGGER.info("Loaded %s existing Bronze event IDs", len(seen_event_ids))

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        group_id=group_id,
    )

    processed = 0
    LOGGER.info("Listening to clickstream topic: %s", topic)

    try:
        for message in consumer:
            event = message.value

            if not isinstance(event, dict):
                raise ValueError(
                    f"Expected JSON object at {message.topic}:{message.partition}:{message.offset}"
                )

            record = prepare_bronze_record(
                event=event,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )

            append_bronze_record(record, output_dir, seen_event_ids)

            # Commit only after the Bronze write or duplicate check succeeds.
            topic_partition = TopicPartition(message.topic, message.partition)
            consumer.commit(
                offsets={topic_partition: OffsetAndMetadata(message.offset + 1, "", -1)}
            )

            processed += 1
            LOGGER.info("Processed clickstream event: %s", record["_batch_id"])

            if max_messages is not None and processed >= max_messages:
                break

    except KeyboardInterrupt:
        LOGGER.info("Clickstream consumer stopped by user.")
    finally:
        consumer.close()

    LOGGER.info("Processed %s clickstream messages", processed)
    return processed


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka clickstream events into Bronze.")
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    processed = consume(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        output_dir=args.output_dir,
        group_id=args.group_id,
        max_messages=args.max_messages,
    )
    print(f"Processed {processed} clickstream messages.")


if __name__ == "__main__":
    main()
