"""Ingest exchange rates from Frankfurter API into Bronze JSONL."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx

DEFAULT_OUTPUT_FILE = Path("data/bronze/fx_rates/raw_fx_rates.jsonl")

API_URL = "https://api.frankfurter.dev/v1/latest"

BASE_CURRENCIES = ("USD", "EUR", "GBP")
QUOTE_CURRENCY = "INR"


def load_existing_rate_keys(output_file: Path) -> set[tuple]:
    """Load existing FX rate keys to prevent duplicate ingestion."""
    existing_keys = set()

    if not output_file.exists():
        return existing_keys

    with output_file.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            record = json.loads(line)
            payload = record["raw_payload"]

            key = (
                payload["rate_date"],
                payload["base_currency"],
                payload["quote_currency"],
            )
            existing_keys.add(key)

    return existing_keys


def ingest_fx_rates(output_file: Path = DEFAULT_OUTPUT_FILE) -> dict:
    """Fetch reference FX rates and append new records to Bronze."""
    output_file.parent.mkdir(parents=True, exist_ok=True)

    existing_keys = load_existing_rate_keys(output_file)

    written = 0
    skipped = 0

    with httpx.Client(timeout=30.0) as client:
        for base_currency in BASE_CURRENCIES:
            response = client.get(
                API_URL,
                params={
                    "base": base_currency,
                    "symbols": QUOTE_CURRENCY,
                },
            )
            response.raise_for_status()

            data = response.json()

            rate_date = data["date"]
            exchange_rate = data["rates"][QUOTE_CURRENCY]

            key = (
                rate_date,
                base_currency,
                QUOTE_CURRENCY,
            )

            if key in existing_keys:
                skipped += 1
                continue

            raw_payload = {
                "rate_date": rate_date,
                "base_currency": base_currency,
                "quote_currency": QUOTE_CURRENCY,
                "exchange_rate": exchange_rate,
            }

            bronze_record = {
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
                "_source_system": "frankfurter_api",
                "_batch_id": f"fx_rates_{rate_date}",
                "raw_payload": raw_payload,
            }

            with output_file.open("a", encoding="utf-8") as file:
                file.write(json.dumps(bronze_record, ensure_ascii=False) + "\n")

            existing_keys.add(key)
            written += 1

    return {
        "records_written": written,
        "records_skipped": skipped,
        "output_file": str(output_file),
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest FX rates into Bronze")
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
    )

    args = parser.parse_args()

    result = ingest_fx_rates(args.output_file)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
