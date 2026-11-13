import json

import pytest

from src.transformations.silver_payments import (
    process_payments,
    transform_payment,
    validate_amount,
)


def make_payment_record(payment_id="PAY000001"):
    return {
        "_source_system": "payment_api",
        "_ingested_at": "2026-11-11T11:00:00Z",
        "raw_payload": {
            "payment_id": payment_id,
            "order_id": "ORD000001",
            "customer_id": 913,
            "payment_method": "credit_card",
            "payment_status": "SUCCESS",
            "amount": 999.00,
            "currency": "INR",
            "transaction_timestamp": "2026-11-11T10:00:00Z",
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


def test_transform_payment_valid():
    result = transform_payment(make_payment_record())

    assert result["payment_id"] == "PAY000001"
    assert result["order_id"] == "ORD000001"
    assert result["customer_id"] == 913
    assert result["payment_status"] == "SUCCESS"
    assert result["amount"] == "999.0"
    assert result["currency"] == "INR"


@pytest.mark.parametrize(
    "status",
    ["PENDING", "SUCCESS", "FAILED", "REFUNDED"],
)
def test_valid_payment_statuses(status):
    record = make_payment_record()
    record["raw_payload"]["payment_status"] = status

    assert transform_payment(record)["payment_status"] == status


def test_invalid_payment_status():
    record = make_payment_record()
    record["raw_payload"]["payment_status"] = "UNKNOWN"

    with pytest.raises(ValueError, match="Invalid payment_status"):
        transform_payment(record)


@pytest.mark.parametrize(
    "method",
    ["credit_card", "debit_card", "wallet", "upi", "net_banking"],
)
def test_valid_payment_methods(method):
    record = make_payment_record()
    record["raw_payload"]["payment_method"] = method

    assert transform_payment(record)["payment_method"] == method


def test_missing_payment_id():
    record = make_payment_record()
    record["raw_payload"]["payment_id"] = ""

    with pytest.raises(ValueError, match="Missing payment_id"):
        transform_payment(record)


def test_missing_order_id():
    record = make_payment_record()
    record["raw_payload"].pop("order_id")

    with pytest.raises(ValueError, match="Missing order_id"):
        transform_payment(record)


def test_invalid_customer_id():
    record = make_payment_record()
    record["raw_payload"]["customer_id"] = -1

    with pytest.raises(ValueError, match="customer_id must be positive"):
        transform_payment(record)


@pytest.mark.parametrize(
    "amount",
    [-100, 0, "invalid", "NaN", "Infinity", None],
)
def test_invalid_payment_amount(amount):
    with pytest.raises(ValueError):
        validate_amount(amount)


def test_amount_preserves_precision():
    assert validate_amount("999.00") == "999.00"
    assert validate_amount("123456789.123456") == "123456789.123456"


def test_invalid_currency():
    record = make_payment_record()
    record["raw_payload"]["currency"] = "USD"

    with pytest.raises(ValueError, match="Invalid currency"):
        transform_payment(record)


def test_invalid_timestamp():
    record = make_payment_record()
    record["raw_payload"]["transaction_timestamp"] = "invalid-date"

    with pytest.raises(ValueError, match="Invalid transaction_timestamp"):
        transform_payment(record)


def test_process_payments_deduplication(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    first = make_payment_record()
    second = make_payment_record()
    second["raw_payload"]["payment_status"] = "REFUNDED"

    write_records(input_path, [first, second])

    result = process_payments(input_path, output_path, rejected_path)
    payments = read_records(output_path)

    assert result["processed"] == 2
    assert result["valid_payments"] == 1
    assert result["rejected"] == 0
    assert payments[0]["payment_status"] == "REFUNDED"


def test_process_payments_rejected_record(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    valid = make_payment_record()
    invalid = make_payment_record(payment_id="PAY000002")
    invalid["raw_payload"]["amount"] = -50

    write_records(input_path, [valid, invalid])

    result = process_payments(input_path, output_path, rejected_path)
    rejected = read_records(rejected_path)

    assert result["processed"] == 2
    assert result["valid_payments"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
    assert rejected[0]["reason"] == "Payment amount must be positive"
