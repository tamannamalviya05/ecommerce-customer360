import base64
import json

from src.ingestion.mysql_cdc_consumer import (
    append_bronze_record,
    decode_connect_decimal,
    normalize_decimal_fields,
    prepare_bronze_record,
)


def test_decode_connect_decimal():
    encoded = base64.b64encode((99900).to_bytes(3, "big", signed=True)).decode()
    assert decode_connect_decimal(encoded) == "999.00"


def test_normalize_decimal_fields():
    event = {
        "payload": {
            "after": {"price": "AYY8", "total_amount": "AYY8"},
            "before": None,
        }
    }

    result = normalize_decimal_fields(event)

    assert result["payload"]["after"]["price"] == "999.00"
    assert result["payload"]["after"]["total_amount"] == "999.00"


def test_prepare_bronze_record():
    event = {"payload": {"op": "c"}}

    record = prepare_bronze_record(
        event,
        "ecommerce.ecommerce.customers",
        0,
        12,
    )

    assert record["_source_system"] == "mysql_debezium"
    assert record["_topic"] == "ecommerce.ecommerce.customers"
    assert record["_partition"] == 0
    assert record["_offset"] == 12
    assert record["_batch_id"] == "ecommerce.ecommerce.customers-0-12"
    assert record["raw_payload"] == event
    assert "_ingested_at" in record


def test_append_bronze_record(tmp_path):
    record = {
        "_topic": "ecommerce.ecommerce.customers",
        "_partition": 0,
        "_offset": 12,
        "_batch_id": "ecommerce.ecommerce.customers-0-12",
        "raw_payload": {"payload": {"op": "c"}},
    }

    output_path = append_bronze_record(record, tmp_path)

    assert output_path.exists()
    saved_record = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved_record == record


def test_load_existing_event_ids(tmp_path):
    from src.ingestion.mysql_cdc_consumer import load_existing_event_ids

    record = {
        "_topic": "ecommerce.ecommerce.customers",
        "_partition": 0,
        "_offset": 12,
    }
    (tmp_path / "customers.jsonl").write_text(
        json.dumps(record) + "\n",
        encoding="utf-8",
    )

    assert load_existing_event_ids(tmp_path) == {("ecommerce.ecommerce.customers", 0, 12)}


def test_append_bronze_record_skips_duplicate(tmp_path):
    from src.ingestion.mysql_cdc_consumer import load_existing_event_ids

    record = {
        "_topic": "ecommerce.ecommerce.customers",
        "_partition": 0,
        "_offset": 12,
        "_batch_id": "ecommerce.ecommerce.customers-0-12",
        "raw_payload": {"payload": {"op": "c"}},
    }
    seen_event_ids = set()

    append_bronze_record(record, tmp_path, seen_event_ids)
    append_bronze_record(record, tmp_path, seen_event_ids)

    output_path = tmp_path / "ecommerce_ecommerce_customers.jsonl"
    assert len(output_path.read_text(encoding="utf-8").splitlines()) == 1
    assert load_existing_event_ids(tmp_path) == {("ecommerce.ecommerce.customers", 0, 12)}
