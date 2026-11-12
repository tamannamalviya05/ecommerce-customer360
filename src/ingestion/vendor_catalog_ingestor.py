import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

DEFAULT_INPUT_FILE = Path("data/vendor/vendor_catalog.json")
DEFAULT_OUTPUT_DIR = Path("data/bronze/vendor_catalog")


def load_existing_feed_record_keys(output_dir: Path) -> set[str]:
    """Load product and feed-date keys from existing Bronze JSONL files."""
    existing_keys = set()

    if not output_dir.exists():
        return existing_keys

    for file_path in output_dir.glob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                record = json.loads(line)
                key = record.get("_feed_record_key")

                if key:
                    existing_keys.add(key)

    return existing_keys


def ingest_vendor_catalog(
    input_file: Path,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    feed_date: date | None = None,
) -> dict:
    """Append vendor catalog records to Bronze with feed-level deduplication."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    feed_date = feed_date or date.today()

    with input_file.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list):
        raise ValueError("Vendor catalog input must contain a JSON array")

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "raw_vendor_catalog.jsonl"
    existing_keys = load_existing_feed_record_keys(output_dir)

    batch_id = f"vendor_catalog_{feed_date.isoformat()}"
    written = 0
    skipped = 0

    with output_file.open("a", encoding="utf-8") as file:
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("Every vendor catalog record must be a JSON object")

            product_id = record.get("product_id")

            if not product_id:
                raise ValueError("Vendor catalog record is missing product_id")

            feed_record_key = f"{feed_date.isoformat()}:{product_id}"

            if feed_record_key in existing_keys:
                skipped += 1
                continue

            bronze_record = {
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
                "_source_system": "vendor_catalog",
                "_batch_id": batch_id,
                "_source_record_id": product_id,
                "_feed_record_key": feed_record_key,
                "_source_file": input_file.name,
                "_feed_date": feed_date.isoformat(),
                "raw_payload": record,
            }

            file.write(json.dumps(bronze_record, ensure_ascii=False) + "\n")

            existing_keys.add(feed_record_key)
            written += 1

    return {
        "batch_id": batch_id,
        "feed_date": feed_date.isoformat(),
        "records_received": len(records),
        "records_written": written,
        "records_skipped": skipped,
        "output_file": str(output_file),
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest vendor catalog JSON into Bronze")
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--feed-date", type=date.fromisoformat)

    args = parser.parse_args()

    result = ingest_vendor_catalog(
        input_file=args.input_file,
        output_dir=args.output_dir,
        feed_date=args.feed_date,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
