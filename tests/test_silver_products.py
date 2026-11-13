import json

import pytest

from src.transformations.silver_products import (
    process_products,
    transform_product,
    validate_price,
)


def make_product_record(operation="c", product_id=1):
    product = {
        "product_id": product_id,
        "product_name": " Test Product ",
        "category": " Electronics ",
        "price": "999.00",
        "stock_quantity": 10,
        "updated_at": "2026-11-11T10:00:00Z",
    }

    return {
        "_ingested_at": "2026-11-11T11:00:00Z",
        "raw_payload": {
            "payload": {
                "op": operation,
                "before": product if operation == "d" else None,
                "after": None if operation == "d" else product,
            }
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


def test_transform_product_insert():
    result = transform_product(make_product_record())

    assert result["product_id"] == 1
    assert result["product_name"] == "Test Product"
    assert result["category"] == "Electronics"
    assert result["price"] == "999.00"
    assert result["stock_quantity"] == 10
    assert result["_deleted"] is False
    assert result["_cdc_operation"] == "c"


def test_transform_product_update():
    record = make_product_record(operation="u")
    record["raw_payload"]["payload"]["after"]["price"] = "1299.50"

    result = transform_product(record)

    assert result["price"] == "1299.50"
    assert result["_cdc_operation"] == "u"


def test_transform_product_snapshot():
    result = transform_product(make_product_record(operation="r"))

    assert result["product_id"] == 1
    assert result["_cdc_operation"] == "r"


def test_transform_product_delete():
    result = transform_product(make_product_record(operation="d"))

    assert result["product_id"] == 1
    assert result["_deleted"] is True
    assert result["_cdc_operation"] == "d"


def test_validate_price_preserves_precision():
    assert validate_price("999.00") == "999.00"
    assert validate_price("123456789.123456") == "123456789.123456"


@pytest.mark.parametrize("price", ["invalid", "-10.00", "NaN", "Infinity", None])
def test_validate_price_rejects_invalid_values(price):
    with pytest.raises(ValueError):
        validate_price(price)


def test_transform_product_invalid_id():
    with pytest.raises(ValueError, match="product_id must be positive"):
        transform_product(make_product_record(product_id=-1))


def test_transform_product_missing_name():
    record = make_product_record()
    record["raw_payload"]["payload"]["after"]["product_name"] = ""

    with pytest.raises(ValueError, match="Missing product_name"):
        transform_product(record)


def test_transform_product_negative_stock():
    record = make_product_record()
    record["raw_payload"]["payload"]["after"]["stock_quantity"] = -5

    with pytest.raises(ValueError, match="stock_quantity cannot be negative"):
        transform_product(record)


def test_transform_product_invalid_timestamp():
    record = make_product_record()
    record["raw_payload"]["payload"]["after"]["updated_at"] = "invalid-date"

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        transform_product(record)


def test_transform_product_unsupported_operation():
    with pytest.raises(ValueError, match="Unsupported CDC operation"):
        transform_product(make_product_record(operation="x"))


def test_process_products_deduplication(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    records = [
        make_product_record(operation="c"),
        make_product_record(operation="u"),
    ]
    records[1]["raw_payload"]["payload"]["after"]["price"] = "1499.00"

    write_records(input_path, records)

    result = process_products(input_path, output_path, rejected_path)
    products = read_records(output_path)

    assert result["processed"] == 2
    assert result["valid_products"] == 1
    assert result["rejected"] == 0
    assert products[0]["price"] == "1499.00"


def test_process_products_delete(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    write_records(
        input_path,
        [
            make_product_record(operation="c"),
            make_product_record(operation="d"),
        ],
    )

    result = process_products(input_path, output_path, rejected_path)

    assert result["processed"] == 2
    assert result["valid_products"] == 0
    assert read_records(output_path) == []


def test_process_products_rejected_record(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    valid = make_product_record()
    invalid = make_product_record(product_id=-1)

    write_records(input_path, [valid, invalid])

    result = process_products(input_path, output_path, rejected_path)
    rejected = read_records(rejected_path)

    assert result["processed"] == 2
    assert result["valid_products"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
