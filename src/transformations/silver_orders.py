import argparse
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/mysql_cdc/ecommerce_ecommerce_orders.jsonl")
DEFAULT_OUTPUT = Path("data/silver/orders.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/orders_rejected.jsonl")


def validate_timestamp(value):
    """Validate an ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing or invalid timestamp")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid timestamp format") from exc

    return value


def validate_amount(value):
    """Validate order amount without losing decimal precision."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid total_amount")

    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid total_amount") from exc

    if not amount.is_finite():
        raise ValueError("Invalid total_amount")

    if amount < 0:
        raise ValueError("total_amount cannot be negative")

    return format(amount, "f")


def transform_order(record):
    """Transform one Debezium order CDC event."""
    if not isinstance(record, dict):
        raise ValueError("Invalid Bronze record")

    raw_payload = record.get("raw_payload")
    if not isinstance(raw_payload, dict):
        raise ValueError("Missing raw payload")

    envelope = raw_payload.get("payload")
    if not isinstance(envelope, dict):
        raise ValueError("Missing CDC payload")

    operation = envelope.get("op")

    if operation not in {"c", "u", "r", "d"}:
        raise ValueError(f"Unsupported CDC operation: {operation}")

    order = envelope.get("before") if operation == "d" else envelope.get("after")

    if not isinstance(order, dict):
        raise ValueError("Missing order data")

    order_id = order.get("order_id")

    if isinstance(order_id, bool) or not isinstance(order_id, int):
        raise ValueError("Invalid order_id")

    if order_id <= 0:
        raise ValueError("order_id must be positive")

    if operation == "d":
        return {
            "order_id": order_id,
            "_deleted": True,
            "_cdc_operation": operation,
            "_ingested_at": record.get("_ingested_at"),
        }

    customer_id = order.get("customer_id")

    if isinstance(customer_id, bool) or not isinstance(customer_id, int):
        raise ValueError("Invalid customer_id")

    if customer_id <= 0:
        raise ValueError("customer_id must be positive")

    order_status = order.get("order_status")

    if not isinstance(order_status, str) or not order_status.strip():
        raise ValueError("Missing order_status")

    order_status = order_status.strip().upper()

    if not all(character.isalnum() or character == "_" for character in order_status):
        raise ValueError("Invalid order_status")

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": order_status,
        "total_amount": validate_amount(order.get("total_amount")),
        "created_at": validate_timestamp(order.get("created_at")),
        "updated_at": validate_timestamp(order.get("updated_at")),
        "_deleted": False,
        "_cdc_operation": operation,
        "_ingested_at": record.get("_ingested_at"),
    }


def process_orders(input_path, output_path, rejected_path):
    """Build the current order snapshot from ordered CDC events."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    rejected_path = Path(rejected_path)

    orders = {}
    rejected = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            processed += 1

            try:
                record = json.loads(line)
                order = transform_order(record)
                order_id = order["order_id"]

                if order["_deleted"]:
                    orders.pop(order_id, None)
                else:
                    orders[order_id] = order

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
        for order in orders.values():
            file.write(json.dumps(order) + "\n")

    with rejected_path.open("w", encoding="utf-8") as file:
        for record in rejected:
            file.write(json.dumps(record) + "\n")

    return {
        "processed": processed,
        "valid_orders": len(orders),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform MySQL CDC orders into Silver")

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_orders(args.input, args.output, args.rejected)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
