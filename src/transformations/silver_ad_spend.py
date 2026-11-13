import argparse
import json
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/ad_spend/raw_ad_spend.jsonl")
DEFAULT_OUTPUT = Path("data/silver/ad_spend.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/ad_spend_rejected.jsonl")

VALID_CHANNELS = {
    "facebook_ads",
    "google_ads",
    "instagram_ads",
    "youtube_ads",
}


def validate_spend_amount(value):
    """Validate advertising spend without losing decimal precision."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid spend_amount")

    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid spend_amount") from exc

    if not amount.is_finite():
        raise ValueError("Invalid spend_amount")

    if amount < 0:
        raise ValueError("spend_amount cannot be negative")

    return format(amount, "f")


def validate_spend_date(value):
    """Validate an ISO-format advertising spend date."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing spend_date")

    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Invalid spend_date") from exc

    return value


def validate_metric(value, name):
    """Validate a non-negative integer advertising metric."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Invalid {name}")

    if value < 0:
        raise ValueError(f"{name} cannot be negative")

    return value


def transform_ad_spend(record):
    """Clean and validate one Bronze advertising spend record."""
    if not isinstance(record, dict):
        raise ValueError("Invalid Bronze record")

    spend = record.get("raw_payload")

    if not isinstance(spend, dict):
        raise ValueError("Missing ad spend payload")

    spend_id = spend.get("spend_id")
    campaign_id = spend.get("campaign_id")
    campaign_name = spend.get("campaign_name")

    if not isinstance(spend_id, str) or not spend_id.strip():
        raise ValueError("Missing spend_id")

    if not isinstance(campaign_id, str) or not campaign_id.strip():
        raise ValueError("Missing campaign_id")

    if not isinstance(campaign_name, str) or not campaign_name.strip():
        raise ValueError("Missing campaign_name")

    channel = spend.get("channel")

    if not isinstance(channel, str) or channel.strip().lower() not in VALID_CHANNELS:
        raise ValueError("Invalid channel")

    currency = spend.get("currency")

    if not isinstance(currency, str) or currency.strip().upper() != "INR":
        raise ValueError("Invalid currency")

    impressions = validate_metric(spend.get("impressions"), "impressions")
    clicks = validate_metric(spend.get("clicks"), "clicks")
    conversions = validate_metric(spend.get("conversions"), "conversions")

    if clicks > impressions:
        raise ValueError("clicks cannot exceed impressions")

    if conversions > clicks:
        raise ValueError("conversions cannot exceed clicks")

    return {
        "spend_id": spend_id.strip(),
        "campaign_id": campaign_id.strip(),
        "campaign_name": campaign_name.strip(),
        "channel": channel.strip().lower(),
        "spend_date": validate_spend_date(spend.get("spend_date")),
        "spend_amount": validate_spend_amount(spend.get("spend_amount")),
        "currency": currency.strip().upper(),
        "impressions": impressions,
        "clicks": clicks,
        "conversions": conversions,
        "_source_system": record.get("_source_system"),
        "_ingested_at": record.get("_ingested_at"),
    }


def process_ad_spend(input_path, output_path, rejected_path):
    """Build Silver advertising records and quarantine invalid rows."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    rejected_path = Path(rejected_path)

    records = {}
    rejected = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            processed += 1

            try:
                bronze_record = json.loads(line)
                spend_record = transform_ad_spend(bronze_record)

                # Keep the last valid occurrence of each spend ID.
                records[spend_record["spend_id"]] = spend_record

            except (ValueError, TypeError, AttributeError) as exc:
                rejected.append(
                    {
                        "line_number": line_number,
                        "reason": str(exc),
                        "raw_line": line.strip(),
                    }
                )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for record in records.values():
            file.write(json.dumps(record) + "\n")

    with rejected_path.open("w", encoding="utf-8") as file:
        for record in rejected:
            file.write(json.dumps(record) + "\n")

    return {
        "processed": processed,
        "valid_ad_spend": len(records),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform Bronze advertising spend into Silver")

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_ad_spend(args.input, args.output, args.rejected)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
