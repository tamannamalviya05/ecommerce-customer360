import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from src.api.payment_api import app, generate_payments
from src.ingestion.payment_api_ingestor import (
    append_bronze_record,
    load_existing_payment_ids,
)

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_payment_pagination():
    response = client.get("/payments", params={"limit": 5, "offset": 0})

    assert response.status_code == 200
    assert len(response.json()["data"]) == 5
    assert response.json()["total"] == 5000


def test_payment_lookup():
    response = client.get("/payments/PAY000001")

    assert response.status_code == 200
    assert response.json()["payment_id"] == "PAY000001"


def test_payment_not_found():
    response = client.get("/payments/INVALID")

    assert response.status_code == 404


def test_invalid_pagination():
    response = client.get("/payments", params={"limit": 0})

    assert response.status_code == 422


def test_generate_payments_is_deterministic():
    fixed_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = generate_payments(count=100, seed=42, base_time=fixed_time)
    second = generate_payments(count=100, seed=42, base_time=fixed_time)

    assert first == second


def test_generate_payments_injects_invalid_records():
    payments = generate_payments(count=100, seed=42)

    invalid_records = [
        payment
        for payment in payments
        if (
            "order_id" not in payment
            or payment["amount"] < 0
            or payment["payment_status"] == "UNKNOWN"
        )
    ]

    assert len(invalid_records) == 2


def test_append_bronze_record(tmp_path):
    payment = {
        "payment_id": "PAYTEST001",
        "order_id": "ORDTEST001",
        "amount": 500.0,
        "currency": "INR",
    }
    seen_ids = set()

    result = append_bronze_record(
        payment,
        tmp_path,
        "test-batch",
        seen_ids,
    )

    assert result is True

    output_file = tmp_path / "raw_payments.jsonl"
    record = json.loads(output_file.read_text(encoding="utf-8"))

    assert record["_source_system"] == "payment_api"
    assert record["_batch_id"] == "test-batch"
    assert record["_source_record_id"] == "PAYTEST001"
    assert record["raw_payload"] == payment


def test_duplicate_payment_is_skipped(tmp_path):
    payment = {"payment_id": "PAYTEST002"}
    seen_ids = set()

    first = append_bronze_record(payment, tmp_path, "batch-1", seen_ids)
    second = append_bronze_record(payment, tmp_path, "batch-1", seen_ids)

    assert first is True
    assert second is False

    output_file = tmp_path / "raw_payments.jsonl"
    assert len(output_file.read_text(encoding="utf-8").splitlines()) == 1


def test_load_existing_payment_ids(tmp_path):
    output_file = tmp_path / "raw_payments.jsonl"
    output_file.write_text(
        '{"_source_record_id":"PAYTEST003"}\n',
        encoding="utf-8",
    )

    existing_ids = load_existing_payment_ids(tmp_path)

    assert "PAYTEST003" in existing_ids
