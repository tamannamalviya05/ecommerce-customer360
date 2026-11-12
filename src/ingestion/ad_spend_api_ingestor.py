import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8001"
DEFAULT_OUTPUT_DIR = Path("data/bronze/ad_spend")


def load_existing_ad_spend_ids(output_dir: Path) -> set[str]:
    """Load ad spend IDs already written to Bronze."""
    existing_ids = set()

    if not output_dir.exists():
        return existing_ids

    for file_path in output_dir.glob("*.jsonl"):
        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    record = json.loads(line)
                    spend_id = record.get("_source_record_id")
                    if spend_id:
                        existing_ids.add(spend_id)

    return existing_ids


def append_bronze_record(
    spend: dict,
    output_dir: Path,
    batch_id: str,
    seen_spend_ids: set[str],
) -> bool:
    """Append one ad spend record, skipping previously ingested IDs."""
    spend_id = spend.get("spend_id")

    if not spend_id:
        raise ValueError("Ad spend record is missing spend_id")

    if spend_id in seen_spend_ids:
        return False

    record = {
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source_system": "ad_spend_api",
        "_batch_id": batch_id,
        "_source_record_id": spend_id,
        "raw_payload": spend,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "raw_ad_spend.jsonl"

    with output_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")

    seen_spend_ids.add(spend_id)
    return True


def ingest_ad_spend(
    base_url: str = DEFAULT_BASE_URL,
    page_size: int = 1000,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    max_records: int | None = None,
) -> dict:
    """Fetch paginated ad spend and append records to Bronze."""
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")

    if max_records is not None and max_records < 1:
        raise ValueError("max_records must be at least 1")

    batch_id = str(uuid4())
    seen_spend_ids = load_existing_ad_spend_ids(output_dir)

    fetched_count = 0
    written_count = 0
    offset = 0

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        while True:
            limit = page_size

            if max_records is not None:
                remaining = max_records - fetched_count
                if remaining <= 0:
                    break
                limit = min(page_size, remaining)

            response = client.get(
                "/ad-spend",
                params={"limit": limit, "offset": offset},
            )
            response.raise_for_status()

            payload = response.json()
            spends = payload.get("data")

            if not isinstance(spends, list):
                raise ValueError("API response does not contain a valid data list")

            if not spends:
                break

            for spend in spends:
                fetched_count += 1

                if append_bronze_record(
                    spend,
                    output_dir,
                    batch_id,
                    seen_spend_ids,
                ):
                    written_count += 1

            offset += len(spends)

            if offset >= payload["total"]:
                break

    return {
        "batch_id": batch_id,
        "fetched": fetched_count,
        "written": written_count,
        "skipped_duplicates": fetched_count - written_count,
        "output_file": str(output_dir / "raw_ad_spend.jsonl"),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Ingest ad spend API records into the Bronze layer"
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-records", type=int, default=None)
    args = parser.parse_args()

    result = ingest_ad_spend(
        base_url=args.base_url,
        page_size=args.page_size,
        output_dir=args.output_dir,
        max_records=args.max_records,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
