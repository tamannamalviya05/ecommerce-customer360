import json
from datetime import date
from pathlib import Path

import pytest

from src.ingestion.vendor_catalog_ingestor import ingest_vendor_catalog


@pytest.fixture
def input_file(tmp_path: Path) -> Path:
    records = [
        {
            "product_id": "PROD000001",
            "sku": "SKU000001",
            "product_name": "Product 1",
            "list_price": 1500.0,
            "currency": "INR",
            "stock_quantity": 10,
        },
        {
            "product_id": "PROD000002",
            "sku": "SKU000002",
            "product_name": "Product 2",
            "list_price": -100.0,
            "currency": "XXX",
            "stock_quantity": -5,
        },
    ]

    path = tmp_path / "vendor_catalog.json"
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def test_ingests_vendor_catalog(input_file: Path, tmp_path: Path):
    output_dir = tmp_path / "bronze"

    result = ingest_vendor_catalog(
        input_file,
        output_dir,
        feed_date=date(2026, 11, 12),
    )

    assert result["records_received"] == 2
    assert result["records_written"] == 2
    assert result["records_skipped"] == 0

    output_file = output_dir / "raw_vendor_catalog.jsonl"
    records = [json.loads(line) for line in output_file.read_text(encoding="utf-8").splitlines()]

    assert len(records) == 2
    assert records[0]["_source_system"] == "vendor_catalog"
    assert records[0]["_batch_id"] == "vendor_catalog_2026-11-12"
    assert records[0]["raw_payload"]["product_id"] == "PROD000001"


def test_skips_duplicate_records_on_same_feed_date(
    input_file: Path,
    tmp_path: Path,
):
    output_dir = tmp_path / "bronze"
    feed_date = date(2026, 11, 12)

    first = ingest_vendor_catalog(input_file, output_dir, feed_date)
    second = ingest_vendor_catalog(input_file, output_dir, feed_date)

    assert first["records_written"] == 2
    assert second["records_written"] == 0
    assert second["records_skipped"] == 2


def test_ingests_same_products_on_new_feed_date(
    input_file: Path,
    tmp_path: Path,
):
    output_dir = tmp_path / "bronze"

    first = ingest_vendor_catalog(
        input_file,
        output_dir,
        date(2026, 11, 12),
    )
    second = ingest_vendor_catalog(
        input_file,
        output_dir,
        date(2026, 11, 13),
    )

    assert first["records_written"] == 2
    assert second["records_written"] == 2

    output_file = output_dir / "raw_vendor_catalog.jsonl"
    records = output_file.read_text(encoding="utf-8").splitlines()

    assert len(records) == 4


def test_rejects_record_without_product_id(
    tmp_path: Path,
):
    input_path = tmp_path / "invalid.json"
    input_path.write_text(
        json.dumps([{"sku": "SKU000001"}]),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="product_id"):
        ingest_vendor_catalog(
            input_path,
            tmp_path / "bronze",
            date(2026, 11, 12),
        )


def test_rejects_non_array_input(tmp_path: Path):
    input_path = tmp_path / "invalid.json"
    input_path.write_text(
        json.dumps({"product_id": "PROD000001"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="JSON array"):
        ingest_vendor_catalog(
            input_path,
            tmp_path / "bronze",
            date(2026, 11, 12),
        )


def test_rejects_missing_input_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        ingest_vendor_catalog(
            tmp_path / "missing.json",
            tmp_path / "bronze",
            date(2026, 11, 12),
        )
