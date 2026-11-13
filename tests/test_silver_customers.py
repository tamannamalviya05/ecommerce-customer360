import json

import pytest

from src.transformations.silver_customers import (
    process_customers,
    transform_customer,
)


def make_customer_record(
    operation="c",
    customer_id=1,
    before=None,
    after=None,
):
    """Create a sample Debezium CDC customer record."""
    customer = {
        "customer_id": customer_id,
        "first_name": " Test ",
        "last_name": " Customer ",
        "email": " TEST@EXAMPLE.COM ",
        "city": "Jaipur",
        "country": "India",
        "created_at": "2026-11-11T10:00:00Z",
        "updated_at": "2026-11-11T10:00:00Z",
    }

    if after is None and operation != "d":
        after = customer

    if before is None and operation == "d":
        before = customer

    return {
        "_ingested_at": "2026-11-11T11:00:00Z",
        "raw_payload": {
            "payload": {
                "op": operation,
                "before": before,
                "after": after,
            }
        },
    }


def test_transform_customer_insert():
    result = transform_customer(make_customer_record())

    assert result["customer_id"] == 1
    assert result["first_name"] == "Test"
    assert result["last_name"] == "Customer"
    assert result["email"] == "test@example.com"
    assert result["_deleted"] is False
    assert result["_cdc_operation"] == "c"


def test_transform_customer_update():
    record = make_customer_record(operation="u")
    record["raw_payload"]["payload"]["after"]["city"] = "Delhi"

    result = transform_customer(record)

    assert result["city"] == "Delhi"
    assert result["_cdc_operation"] == "u"


def test_transform_customer_snapshot():
    result = transform_customer(make_customer_record(operation="r"))

    assert result["customer_id"] == 1
    assert result["_cdc_operation"] == "r"


def test_transform_customer_delete():
    result = transform_customer(make_customer_record(operation="d"))

    assert result["customer_id"] == 1
    assert result["_deleted"] is True
    assert result["_cdc_operation"] == "d"


def test_transform_customer_invalid_id():
    record = make_customer_record(customer_id=-1)

    with pytest.raises(ValueError, match="customer_id must be positive"):
        transform_customer(record)


def test_transform_customer_invalid_email():
    record = make_customer_record()
    record["raw_payload"]["payload"]["after"]["email"] = "invalid-email"

    with pytest.raises(ValueError, match="Invalid email"):
        transform_customer(record)


def test_transform_customer_missing_name():
    record = make_customer_record()
    record["raw_payload"]["payload"]["after"]["first_name"] = ""

    with pytest.raises(ValueError, match="Missing required customer fields"):
        transform_customer(record)


def test_transform_customer_invalid_timestamp():
    record = make_customer_record()
    record["raw_payload"]["payload"]["after"]["created_at"] = "not-a-date"

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        transform_customer(record)


def test_transform_customer_unsupported_operation():
    record = make_customer_record(operation="x")

    with pytest.raises(ValueError, match="Unsupported CDC operation"):
        transform_customer(record)


def test_process_customers_deduplication(tmp_path):
    input_path = tmp_path / "customers_bronze.jsonl"
    output_path = tmp_path / "customers_silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    records = [
        make_customer_record(operation="c"),
        make_customer_record(operation="u"),
    ]
    records[1]["raw_payload"]["payload"]["after"]["city"] = "Mumbai"

    input_path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )

    result = process_customers(input_path, output_path, rejected_path)

    customers = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]

    assert result["processed"] == 2
    assert result["valid_customers"] == 1
    assert result["rejected"] == 0
    assert customers[0]["city"] == "Mumbai"


def test_process_customers_delete(tmp_path):
    input_path = tmp_path / "customers_bronze.jsonl"
    output_path = tmp_path / "customers_silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    records = [
        make_customer_record(operation="c"),
        make_customer_record(operation="d"),
    ]

    input_path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )

    result = process_customers(input_path, output_path, rejected_path)

    assert result["processed"] == 2
    assert result["valid_customers"] == 0
    assert output_path.read_text(encoding="utf-8") == ""


def test_process_customers_rejected_record(tmp_path):
    input_path = tmp_path / "customers_bronze.jsonl"
    output_path = tmp_path / "customers_silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    valid_record = make_customer_record()
    invalid_record = make_customer_record(customer_id=-1)

    input_path.write_text(
        json.dumps(valid_record) + "\n" + json.dumps(invalid_record) + "\n",
        encoding="utf-8",
    )

    result = process_customers(input_path, output_path, rejected_path)

    rejected = [json.loads(line) for line in rejected_path.read_text(encoding="utf-8").splitlines()]

    assert result["processed"] == 2
    assert result["valid_customers"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
