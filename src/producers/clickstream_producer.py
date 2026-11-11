"""Generate and publish synthetic e-commerce clickstream events to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

LOGGER = logging.getLogger(__name__)

DEFAULT_TOPIC = "ecommerce.clickstream"

EVENT_TYPES = (
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
)

DEVICES = ("desktop", "mobile", "tablet")
REFERRERS = ("direct", "google", "facebook", "email", "instagram")


def generate_events(count: int, seed: int | None = None):
    """Generate realistic synthetic clickstream events."""
    if count < 1:
        raise ValueError("count must be at least 1")

    rng = random.Random(seed)
    generated = 0

    while generated < count:
        customer_id = rng.randint(1, 5000)
        session_id = str(uuid.uuid4())
        device_type = rng.choice(DEVICES)
        referrer = rng.choice(REFERRERS)

        session_length = rng.randint(3, 10)

        for _ in range(min(session_length, count - generated)):
            event_type = rng.choices(
                EVENT_TYPES,
                weights=(35, 30, 15, 5, 10, 5),
                k=1,
            )[0]

            product_id = (
                rng.randint(1, 1000)
                if event_type
                in {
                    "product_view",
                    "add_to_cart",
                    "remove_from_cart",
                    "purchase",
                }
                else None
            )

            page = f"/products/{product_id}" if product_id is not None else "/"

            event: dict[str, Any] = {
                "event_id": str(uuid.uuid4()),
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "customer_id": customer_id,
                "session_id": session_id,
                "event_type": event_type,
                "page_url": page,
                "product_id": product_id,
                "referrer": referrer,
                "device_type": device_type,
            }

            yield event
            generated += 1


def publish_events(
    count: int,
    bootstrap_servers: str = "localhost:29092",
    topic: str = DEFAULT_TOPIC,
    seed: int | None = None,
) -> int:
    """Publish generated events to Kafka and wait for acknowledgements."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )

    futures = []

    try:
        for event in generate_events(count, seed):
            future = producer.send(
                topic,
                key=event["customer_id"],
                value=event,
            )
            futures.append(future)

        producer.flush()

        for future in futures:
            future.get(timeout=30)

        LOGGER.info("Successfully published %s events to %s", count, topic)
        return count

    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce clickstream events."
    )
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    published = publish_events(
        count=args.count,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        seed=args.seed,
    )
    print(f"Published {published} clickstream events.")


if __name__ == "__main__":
    main()
