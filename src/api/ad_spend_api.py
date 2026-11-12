import random
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException, Query

app = FastAPI(
    title="E-commerce Ad Spend API",
    description="Mock advertising campaign spend API for the Customer 360 pipeline",
    version="1.0.0",
)


def generate_ad_spend(
    count: int = 1000,
    seed: int = 42,
    base_date: date | None = None,
) -> list[dict]:
    """Generate deterministic mock advertising spend with 2% invalid records."""
    if count < 1:
        raise ValueError("count must be at least 1")

    rng = random.Random(seed)

    channels = ["google_ads", "facebook_ads", "instagram_ads", "youtube_ads"]
    campaigns = [(f"CMP{i:03d}", f"Campaign {i:03d}") for i in range(1, 21)]

    base_date = base_date or date.today()
    records = []

    for i in range(1, count + 1):
        campaign_id, campaign_name = rng.choice(campaigns)
        impressions = rng.randint(1000, 100000)
        clicks = rng.randint(0, impressions)
        conversions = rng.randint(0, clicks)

        record = {
            "spend_id": f"ADS{i:06d}",
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "channel": rng.choice(channels),
            "spend_date": (base_date - timedelta(days=rng.randint(0, 89))).isoformat(),
            "spend_amount": round(rng.uniform(500, 25000), 2),
            "currency": "INR",
            "impressions": impressions,
            "clicks": clicks,
            "conversions": conversions,
        }

        # Inject reproducible invalid records at a 2% rate.
        if i % 50 == 0:
            defect = rng.choice(
                ["missing_campaign", "negative_spend", "invalid_currency", "invalid_clicks"]
            )

            if defect == "missing_campaign":
                record.pop("campaign_id")
            elif defect == "negative_spend":
                record["spend_amount"] = -abs(record["spend_amount"])
            elif defect == "invalid_currency":
                record["currency"] = "XXX"
            else:
                record["clicks"] = record["impressions"] + 1

        records.append(record)

    return records


AD_SPEND = generate_ad_spend()


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "ad-spend-api"}


@app.get("/ad-spend")
def get_ad_spend(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Return paginated advertising spend records."""
    return {
        "data": AD_SPEND[offset : offset + limit],
        "limit": limit,
        "offset": offset,
        "total": len(AD_SPEND),
    }


@app.get("/ad-spend/{spend_id}")
def get_ad_spend_record(spend_id: str):
    """Return one advertising spend record by its identifier."""
    for record in AD_SPEND:
        if record["spend_id"] == spend_id:
            return record

    raise HTTPException(status_code=404, detail="Ad spend record not found")
