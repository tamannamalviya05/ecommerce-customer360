import json

import pytest

from src.transformations.silver_orders import (
    process_orders,
    transform_order,
    validate_amount,
)


def make_order_record(operation="c", order_id=1):
    order = {
        "order_id": order_id,
        "customer_id": 100,
        "order_status": " placed ",
        "total_amount": "999.00",
        "created_at": "2026-11-11T10:00:00Z",
        "updated_at": "2026-11-11T10:00:00Z",
    }

    return {
        "_ingested_at": "2026-11-11T11:00:00Z",
        "raw_payload": {
            "payload": {
                "op": operation,
                "before": order if operation == "d" else None,
                "after": None if operation == "d" else order,
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


def test_transform_order_insert():
    result = transform_order(make_order_record())

    assert result["order_id"] == 1
    assert result["customer_id"] == 100
    assert result["order_status"] == "PLACED"
    assert result["total_amount"] == "999.00"
    assert result["_deleted"] is False
    assert result["_cdc_operation"] == "c"


def test_transform_order_update():
    record = make_order_record(operation="u")
    record["raw_payload"]["payload"]["after"]["order_status"] = "SHIPPED"

    result = transform_order(record)

    assert result["order_status"] == "SHIPPED"
    assert result["_cdc_operation"] == "u"


def test_transform_order_snapshot():
    result = transform_order(make_order_record(operation="r"))

    assert result["order_id"] == 1
    assert result["_cdc_operation"] == "r"


def test_transform_order_delete():
    result = transform_order(make_order_record(operation="d"))

    assert result["order_id"] == 1
    assert result["_deleted"] is True
    assert result["_cdc_operation"] == "d"


def test_validate_amount_preserves_precision():
    assert validate_amount("999.00") == "999.00"
    assert validate_amount("123456789.123456") == "123456789.123456"


@pytest.mark.parametrize(
    "amount",
    ["invalid", "-10.00", "NaN", "Infinity", None],
)
def test_validate_amount_rejects_invalid_values(amount):
    with pytest.raises(ValueError):
        validate_amount(amount)


def test_transform_order_invalid_id():
    with pytest.raises(ValueError, match="order_id must be positive"):
        transform_order(make_order_record(order_id=-1))


def test_transform_order_invalid_customer_id():
    record = make_order_record()
    record["raw_payload"]["payload"]["after"]["customer_id"] = -5

    with pytest.raises(ValueError, match="customer_id must be positive"):
        transform_order(record)


def test_transform_order_missing_status():
    record = make_order_record()
    record["raw_payload"]["payload"]["after"]["order_status"] = ""

    with pytest.raises(ValueError, match="Missing order_status"):
        transform_order(record)


def test_transform_order_invalid_status():
    record = make_order_record()
    record["raw_payload"]["payload"]["after"]["order_status"] = "PLACED!"

    with pytest.raises(ValueError, match="Invalid order_status"):
        transform_order(record)


def test_transform_order_invalid_timestamp():
    record = make_order_record()
    record["raw_payload"]["payload"]["after"]["created_at"] = "invalid-date"

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        transform_order(record)


def test_transform_order_unsupported_operation():
    with pytest.raises(ValueError, match="Unsupported CDC operation"):
        transform_order(make_order_record(operation="x"))


def test_process_orders_deduplication(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    records = [
        make_order_record(operation="c"),
        make_order_record(operation="u"),
    ]
    records[1]["raw_payload"]["payload"]["after"]["order_status"] = "DELIVERED"

    write_records(input_path, records)

    result = process_orders(input_path, output_path, rejected_path)
    orders = read_records(output_path)

    assert result["processed"] == 2
    assert result["valid_orders"] == 1
    assert result["rejected"] == 0
    assert orders[0]["order_status"] == "DELIVERED"


def test_process_orders_delete(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    write_records(
        input_path,
        [
            make_order_record(operation="c"),
            make_order_record(operation="d"),
        ],
    )

    result = process_orders(input_path, output_path, rejected_path)

    assert result["processed"] == 2
    assert result["valid_orders"] == 0
    assert read_records(output_path) == []


def test_process_orders_rejected_record(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    write_records(
        input_path,
        [
            make_order_record(),
            make_order_record(order_id=-1),
        ],
    )

    result = process_orders(input_path, output_path, rejected_path)
    rejected = read_records(rejected_path)

    assert result["processed"] == 2
    assert result["valid_orders"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
