import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query

app = FastAPI(
    title="E-commerce Payment API",
    description="Mock payment transaction API for the Customer 360 pipeline",
    version="1.0.0",
)


def generate_payments(
    count: int = 5000, seed: int = 42, base_time: datetime | None = None
) -> list[dict]:
    """Generate deterministic mock payment records with 2% invalid records."""
    rng = random.Random(seed)
    payments = []
    payment_methods = ["credit_card", "debit_card", "upi", "net_banking", "wallet"]
    payment_statuses = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]

    base_time = base_time or datetime.now(timezone.utc)

    for i in range(1, count + 1):
        payment = {
            "payment_id": f"PAY{i:06d}",
            "order_id": f"ORD{i:06d}",
            "customer_id": rng.randint(1, 5000),
            "payment_method": rng.choice(payment_methods),
            "payment_status": rng.choice(payment_statuses),
            "amount": round(rng.uniform(100, 15000), 2),
            "currency": "INR",
            "transaction_timestamp": (
                base_time - timedelta(minutes=rng.randint(0, 525600))
            ).isoformat(),
        }

        # Inject invalid records at a reproducible 2% rate.
        if i % 50 == 0:
            defect = rng.choice(["missing_order", "negative_amount", "invalid_status"])

            if defect == "missing_order":
                payment.pop("order_id")
            elif defect == "negative_amount":
                payment["amount"] = -abs(payment["amount"])
            else:
                payment["payment_status"] = "UNKNOWN"

        payments.append(payment)

    return payments


PAYMENTS = generate_payments()


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "payment-api"}


@app.get("/payments")
def get_payments(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """Return paginated payment transactions."""
    return {
        "data": PAYMENTS[offset : offset + limit],
        "limit": limit,
        "offset": offset,
        "total": len(PAYMENTS),
    }


@app.get("/payments/{payment_id}")
def get_payment(payment_id: str):
    """Return one payment by its identifier."""
    for payment in PAYMENTS:
        if payment["payment_id"] == payment_id:
            return payment

    raise HTTPException(status_code=404, detail="Payment not found")
