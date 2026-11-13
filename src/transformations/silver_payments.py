import argparse
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/payments/raw_payments.jsonl")
DEFAULT_OUTPUT = Path("data/silver/payments.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/payments_rejected.jsonl")

VALID_STATUSES = {"PENDING", "SUCCESS", "FAILED", "REFUNDED"}
VALID_METHODS = {
    "credit_card",
    "debit_card",
    "wallet",
    "upi",
    "net_banking",
}


def validate_timestamp(value):
    """Validate a payment timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing transaction_timestamp")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid transaction_timestamp") from exc

    return value


def validate_amount(value):
    """Validate amount while preserving decimal precision."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid payment amount")

    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid payment amount") from exc

    if not amount.is_finite():
        raise ValueError("Invalid payment amount")

    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    return format(amount, "f")


def transform_payment(record):
    """Clean and validate one Bronze payment record."""
    if not isinstance(record, dict):
        raise ValueError("Invalid Bronze record")

    payment = record.get("raw_payload")

    if not isinstance(payment, dict):
        raise ValueError("Missing payment payload")

    payment_id = payment.get("payment_id")
    order_id = payment.get("order_id")
    customer_id = payment.get("customer_id")

    if not isinstance(payment_id, str) or not payment_id.strip():
        raise ValueError("Missing payment_id")

    if not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("Missing order_id")

    if isinstance(customer_id, bool) or not isinstance(customer_id, int):
        raise ValueError("Invalid customer_id")

    if customer_id <= 0:
        raise ValueError("customer_id must be positive")

    method = payment.get("payment_method")

    if not isinstance(method, str) or method.strip().lower() not in VALID_METHODS:
        raise ValueError("Invalid payment_method")

    status = payment.get("payment_status")

    if not isinstance(status, str) or status.strip().upper() not in VALID_STATUSES:
        raise ValueError("Invalid payment_status")

    currency = payment.get("currency")

    if not isinstance(currency, str) or currency.strip().upper() != "INR":
        raise ValueError("Invalid currency")

    return {
        "payment_id": payment_id.strip(),
        "order_id": order_id.strip(),
        "customer_id": customer_id,
        "payment_method": method.strip().lower(),
        "payment_status": status.strip().upper(),
        "amount": validate_amount(payment.get("amount")),
        "currency": currency.strip().upper(),
        "transaction_timestamp": validate_timestamp(payment.get("transaction_timestamp")),
        "_source_system": record.get("_source_system"),
        "_ingested_at": record.get("_ingested_at"),
    }


def process_payments(input_path, output_path, rejected_path):
    """Build Silver payments and quarantine invalid records."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    rejected_path = Path(rejected_path)

    payments = {}
    rejected = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            processed += 1

            try:
                record = json.loads(line)
                payment = transform_payment(record)
                payment_id = payment["payment_id"]

                # Keep the last valid occurrence of each payment ID.
                payments[payment_id] = payment

            except (ValueError, TypeError, AttributeError) as exc:
                rejected.append(
                    {
                        "line_number": line_number,
                        "reason": str(exc),
                        "raw_line": line.strip(),
                    }
                )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for payment in payments.values():
            file.write(json.dumps(payment) + "\n")

    with rejected_path.open("w", encoding="utf-8") as file:
        for record in rejected:
            file.write(json.dumps(record) + "\n")

    return {
        "processed": processed,
        "valid_payments": len(payments),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform Bronze payments into Silver")

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_payments(args.input, args.output, args.rejected)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
