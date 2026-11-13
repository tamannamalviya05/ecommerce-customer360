import json

import pytest

from src.transformations.silver_vendor_catalog import (
    process_vendor_catalog,
    transform_vendor_product,
    validate_price,
    validate_timestamp,
)


def make_vendor_record(product_id="PROD000001"):
    return {
        "_source_system": "vendor_catalog",
        "_ingested_at": "2026-11-12T06:33:58Z",
        "raw_payload": {
            "product_id": product_id,
            "sku": "SKU000001",
            "product_name": "Test Product",
            "category": "Electronics",
            "brand": "Zenith",
            "vendor_id": "VEN008",
            "list_price": "11238.22",
            "currency": "INR",
            "stock_quantity": 754,
            "updated_at": "2026-10-23T09:00:00+00:00",
        },
    }


def write_records(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def read_records(path):
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def test_transform_valid_vendor_product():
    result = transform_vendor_product(make_vendor_record())

    assert result["product_id"] == "PROD000001"
    assert result["sku"] == "SKU000001"
    assert result["vendor_id"] == "VEN008"
    assert result["list_price"] == "11238.22"
    assert result["stock_quantity"] == 754
    assert result["currency"] == "INR"


@pytest.mark.parametrize(
    "field",
    [
        "product_id",
        "sku",
        "product_name",
        "category",
        "brand",
        "vendor_id",
    ],
)
def test_missing_required_fields(field):
    record = make_vendor_record()
    record["raw_payload"][field] = ""

    with pytest.raises(ValueError, match=f"Missing {field}"):
        transform_vendor_product(record)


def test_text_fields_are_trimmed():
    record = make_vendor_record()
    record["raw_payload"]["product_name"] = "  Test Product  "

    result = transform_vendor_product(record)

    assert result["product_name"] == "Test Product"


def test_price_preserves_precision():
    assert validate_price("11238.22") == "11238.22"
    assert validate_price("123456789.123456") == "123456789.123456"


def test_zero_price_allowed():
    assert validate_price(0) == "0"


@pytest.mark.parametrize(
    "price",
    [-100, "invalid", "NaN", "Infinity", None, True],
)
def test_invalid_price(price):
    with pytest.raises(ValueError):
        validate_price(price)


@pytest.mark.parametrize(
    "stock",
    [-1, 1.5, "100", None, True],
)
def test_invalid_stock_quantity(stock):
    record = make_vendor_record()
    record["raw_payload"]["stock_quantity"] = stock

    with pytest.raises(ValueError):
        transform_vendor_product(record)


def test_zero_stock_allowed():
    record = make_vendor_record()
    record["raw_payload"]["stock_quantity"] = 0

    result = transform_vendor_product(record)

    assert result["stock_quantity"] == 0


def test_invalid_currency():
    record = make_vendor_record()
    record["raw_payload"]["currency"] = "USD"

    with pytest.raises(ValueError, match="Invalid currency"):
        transform_vendor_product(record)


def test_valid_timestamp():
    assert validate_timestamp("2026-10-23T09:00:00+00:00") == "2026-10-23T09:00:00+00:00"


@pytest.mark.parametrize(
    "timestamp",
    ["invalid", "", None, "2026-13-01"],
)
def test_invalid_timestamp(timestamp):
    with pytest.raises(ValueError):
        validate_timestamp(timestamp)


def test_process_vendor_catalog_deduplication(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    first = make_vendor_record()
    second = make_vendor_record()
    second["raw_payload"]["list_price"] = "999.99"

    write_records(input_path, [first, second])

    result = process_vendor_catalog(input_path, output_path, rejected_path)
    products = read_records(output_path)

    assert result["processed"] == 2
    assert result["valid_vendor_products"] == 1
    assert result["rejected"] == 0
    assert products[0]["list_price"] == "999.99"


def test_process_vendor_catalog_rejected_record(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    valid = make_vendor_record()
    invalid = make_vendor_record(product_id="PROD000002")
    invalid["raw_payload"]["stock_quantity"] = -5

    write_records(input_path, [valid, invalid])

    result = process_vendor_catalog(input_path, output_path, rejected_path)
    rejected = read_records(rejected_path)

    assert result["processed"] == 2
    assert result["valid_vendor_products"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
    assert rejected[0]["reason"] == "stock_quantity cannot be negative"
