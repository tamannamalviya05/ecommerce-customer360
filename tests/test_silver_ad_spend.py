import json

import pytest

from src.transformations.silver_ad_spend import (
    process_ad_spend,
    transform_ad_spend,
    validate_metric,
    validate_spend_amount,
    validate_spend_date,
)


def make_ad_spend_record(spend_id="ADS000001"):
    return {
        "_source_system": "ad_spend_api",
        "_ingested_at": "2026-11-12T05:10:45Z",
        "raw_payload": {
            "spend_id": spend_id,
            "campaign_id": "CMP001",
            "campaign_name": "Campaign 001",
            "channel": "google_ads",
            "spend_date": "2026-11-12",
            "spend_amount": "1500.50",
            "currency": "INR",
            "impressions": 10000,
            "clicks": 2000,
            "conversions": 100,
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


def test_transform_valid_ad_spend():
    result = transform_ad_spend(make_ad_spend_record())

    assert result["spend_id"] == "ADS000001"
    assert result["campaign_id"] == "CMP001"
    assert result["channel"] == "google_ads"
    assert result["spend_amount"] == "1500.50"
    assert result["impressions"] == 10000
    assert result["clicks"] == 2000
    assert result["conversions"] == 100


@pytest.mark.parametrize(
    "channel",
    ["facebook_ads", "google_ads", "instagram_ads", "youtube_ads"],
)
def test_valid_channels(channel):
    record = make_ad_spend_record()
    record["raw_payload"]["channel"] = channel

    assert transform_ad_spend(record)["channel"] == channel


def test_invalid_channel():
    record = make_ad_spend_record()
    record["raw_payload"]["channel"] = "unknown_ads"

    with pytest.raises(ValueError, match="Invalid channel"):
        transform_ad_spend(record)


@pytest.mark.parametrize(
    "field",
    ["spend_id", "campaign_id", "campaign_name"],
)
def test_missing_required_fields(field):
    record = make_ad_spend_record()
    record["raw_payload"][field] = ""

    with pytest.raises(ValueError, match="Missing"):
        transform_ad_spend(record)


def test_spend_amount_preserves_precision():
    assert validate_spend_amount("1500.50") == "1500.50"
    assert validate_spend_amount("123456789.123456") == "123456789.123456"


@pytest.mark.parametrize(
    "amount",
    [-100, "invalid", "NaN", "Infinity", None],
)
def test_invalid_spend_amount(amount):
    with pytest.raises(ValueError):
        validate_spend_amount(amount)


def test_zero_spend_is_allowed():
    assert validate_spend_amount(0) == "0"


@pytest.mark.parametrize(
    "value",
    [-1, 1.5, "100", None, True],
)
def test_invalid_metric(value):
    with pytest.raises(ValueError):
        validate_metric(value, "clicks")


def test_clicks_cannot_exceed_impressions():
    record = make_ad_spend_record()
    record["raw_payload"]["clicks"] = 10001

    with pytest.raises(ValueError, match="clicks cannot exceed impressions"):
        transform_ad_spend(record)


def test_conversions_cannot_exceed_clicks():
    record = make_ad_spend_record()
    record["raw_payload"]["conversions"] = 2001

    with pytest.raises(ValueError, match="conversions cannot exceed clicks"):
        transform_ad_spend(record)


def test_invalid_currency():
    record = make_ad_spend_record()
    record["raw_payload"]["currency"] = "USD"

    with pytest.raises(ValueError, match="Invalid currency"):
        transform_ad_spend(record)


def test_valid_spend_date():
    assert validate_spend_date("2026-11-12") == "2026-11-12"


@pytest.mark.parametrize(
    "value",
    ["invalid", "2026-13-01", "", None],
)
def test_invalid_spend_date(value):
    with pytest.raises(ValueError):
        validate_spend_date(value)


def test_process_ad_spend_deduplication(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    first = make_ad_spend_record()
    second = make_ad_spend_record()
    second["raw_payload"]["spend_amount"] = "2500.75"

    write_records(input_path, [first, second])

    result = process_ad_spend(input_path, output_path, rejected_path)
    records = read_records(output_path)

    assert result["processed"] == 2
    assert result["valid_ad_spend"] == 1
    assert result["rejected"] == 0
    assert records[0]["spend_amount"] == "2500.75"


def test_process_ad_spend_rejected_record(tmp_path):
    input_path = tmp_path / "bronze.jsonl"
    output_path = tmp_path / "silver.jsonl"
    rejected_path = tmp_path / "rejected.jsonl"

    valid = make_ad_spend_record()
    invalid = make_ad_spend_record(spend_id="ADS000002")
    invalid["raw_payload"]["spend_amount"] = -500

    write_records(input_path, [valid, invalid])

    result = process_ad_spend(input_path, output_path, rejected_path)
    rejected = read_records(rejected_path)

    assert result["processed"] == 2
    assert result["valid_ad_spend"] == 1
    assert result["rejected"] == 1
    assert rejected[0]["line_number"] == 2
    assert rejected[0]["reason"] == "spend_amount cannot be negative"
