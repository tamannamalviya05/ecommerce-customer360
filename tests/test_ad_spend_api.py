import json
from datetime import date

import httpx
from fastapi.testclient import TestClient

from src.api.ad_spend_api import app, generate_ad_spend
from src.ingestion.ad_spend_api_ingestor import (
    append_bronze_record,
    ingest_ad_spend,
    load_existing_ad_spend_ids,
)

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_ad_spend_pagination():
    response = client.get("/ad-spend", params={"limit": 5, "offset": 0})

    assert response.status_code == 200
    assert len(response.json()["data"]) == 5
    assert response.json()["total"] == 1000


def test_ad_spend_lookup():
    response = client.get("/ad-spend/ADS000001")

    assert response.status_code == 200
    assert response.json()["spend_id"] == "ADS000001"


def test_ad_spend_not_found():
    response = client.get("/ad-spend/INVALID")

    assert response.status_code == 404


def test_invalid_pagination():
    response = client.get("/ad-spend", params={"limit": 0})

    assert response.status_code == 422


def test_generate_ad_spend_is_deterministic():
    fixed_date = date(2026, 1, 1)

    first = generate_ad_spend(count=100, seed=42, base_date=fixed_date)
    second = generate_ad_spend(count=100, seed=42, base_date=fixed_date)

    assert first == second


def test_generate_ad_spend_injects_invalid_records():
    spends = generate_ad_spend(
        count=100,
        seed=42,
        base_date=date(2026, 1, 1),
    )

    invalid_records = [
        spend
        for spend in spends
        if (
            "campaign_id" not in spend
            or spend["spend_amount"] < 0
            or spend["currency"] != "INR"
            or spend["clicks"] > spend["impressions"]
        )
    ]

    assert len(invalid_records) == 2


def test_append_bronze_record(tmp_path):
    spend = {
        "spend_id": "ADSTEST001",
        "campaign_id": "CMP001",
        "spend_amount": 500.0,
        "currency": "INR",
    }
    seen_ids = set()

    result = append_bronze_record(
        spend,
        tmp_path,
        "test-batch",
        seen_ids,
    )

    assert result is True

    output_file = tmp_path / "raw_ad_spend.jsonl"
    record = json.loads(output_file.read_text(encoding="utf-8"))

    assert record["_source_system"] == "ad_spend_api"
    assert record["_batch_id"] == "test-batch"
    assert record["_source_record_id"] == "ADSTEST001"
    assert record["raw_payload"] == spend
    assert "_ingested_at" in record


def test_duplicate_ad_spend_is_skipped(tmp_path):
    spend = {"spend_id": "ADSTEST002"}
    seen_ids = set()

    first = append_bronze_record(spend, tmp_path, "batch-1", seen_ids)
    second = append_bronze_record(spend, tmp_path, "batch-1", seen_ids)

    assert first is True
    assert second is False

    output_file = tmp_path / "raw_ad_spend.jsonl"
    assert len(output_file.read_text(encoding="utf-8").splitlines()) == 1


def test_load_existing_ad_spend_ids(tmp_path):
    output_file = tmp_path / "raw_ad_spend.jsonl"
    output_file.write_text(
        '{"_source_record_id":"ADSTEST003"}\n',
        encoding="utf-8",
    )

    existing_ids = load_existing_ad_spend_ids(tmp_path)

    assert "ADSTEST003" in existing_ids


def test_ingest_ad_spend_with_pagination_and_deduplication(
    tmp_path,
    monkeypatch,
):
    records = [{"spend_id": f"ADSTEST{i:03d}", "spend_amount": 500.0} for i in range(1, 6)]

    def handler(request):
        offset = int(request.url.params.get("offset", 0))
        limit = int(request.url.params.get("limit", 2))

        return httpx.Response(
            200,
            json={
                "data": records[offset : offset + limit],
                "limit": limit,
                "offset": offset,
                "total": len(records),
            },
        )

    import src.ingestion.ad_spend_api_ingestor as ingestor

    original_client = httpx.Client

    def mock_client(*args, **kwargs):
        return original_client(
            base_url=kwargs.get("base_url"),
            transport=httpx.MockTransport(handler),
            timeout=kwargs.get("timeout", 30.0),
        )

    monkeypatch.setattr(ingestor.httpx, "Client", mock_client)

    result = ingest_ad_spend(
        base_url="http://testserver",
        page_size=2,
        output_dir=tmp_path,
    )

    assert result["fetched"] == 5
    assert result["written"] == 5
    assert result["skipped_duplicates"] == 0

    second_result = ingest_ad_spend(
        base_url="http://testserver",
        page_size=2,
        output_dir=tmp_path,
    )

    assert second_result["fetched"] == 5
    assert second_result["written"] == 0
    assert second_result["skipped_duplicates"] == 5

    output_file = tmp_path / "raw_ad_spend.jsonl"
    assert len(output_file.read_text(encoding="utf-8").splitlines()) == 5
