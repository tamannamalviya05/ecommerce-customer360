import argparse
import json
from datetime import datetime
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/mysql_cdc/ecommerce_ecommerce_customers.jsonl")
DEFAULT_OUTPUT = Path("data/silver/customers.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/customers_rejected.jsonl")


def validate_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing or invalid timestamp")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid timestamp format") from exc

    return value


def transform_customer(record):
    """Transform a single Debezium CDC customer event."""
    envelope = record.get("raw_payload", {}).get("payload")

    if not isinstance(envelope, dict):
        raise ValueError("Missing CDC payload")

    operation = envelope.get("op")

    if operation not in {"c", "u", "r", "d"}:
        raise ValueError(f"Unsupported CDC operation: {operation}")

    customer = envelope.get("before") if operation == "d" else envelope.get("after")

    if not isinstance(customer, dict):
        raise ValueError("Missing customer data")

    customer_id = customer.get("customer_id")

    if isinstance(customer_id, bool) or not isinstance(customer_id, int):
        raise ValueError("Invalid customer_id")

    if customer_id <= 0:
        raise ValueError("customer_id must be positive")

    if operation == "d":
        return {
            "customer_id": customer_id,
            "_deleted": True,
            "_cdc_operation": operation,
            "_ingested_at": record.get("_ingested_at"),
        }

    first_name = customer.get("first_name")
    last_name = customer.get("last_name")
    email = customer.get("email")

    if not all(
        isinstance(value, str) and value.strip() for value in (first_name, last_name, email)
    ):
        raise ValueError("Missing required customer fields")

    if "@" not in email or "." not in email.split("@")[-1]:
        raise ValueError("Invalid email")

    return {
        "customer_id": customer_id,
        "first_name": first_name.strip(),
        "last_name": last_name.strip(),
        "email": email.strip().lower(),
        "city": (customer.get("city") or "").strip(),
        "country": (customer.get("country") or "").strip(),
        "created_at": validate_timestamp(customer.get("created_at")),
        "updated_at": validate_timestamp(customer.get("updated_at")),
        "_deleted": False,
        "_cdc_operation": operation,
        "_ingested_at": record.get("_ingested_at"),
    }


def process_customers(input_path, output_path, rejected_path):
    """Build a current customer snapshot from ordered CDC events."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    rejected_path = Path(rejected_path)

    customers = {}
    rejected = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            processed += 1

            try:
                record = json.loads(line)
                customer = transform_customer(record)
                customer_id = customer["customer_id"]

                if customer["_deleted"]:
                    customers.pop(customer_id, None)
                else:
                    customers[customer_id] = customer

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
        for customer in customers.values():
            file.write(json.dumps(customer) + "\n")

    with rejected_path.open("w", encoding="utf-8") as file:
        for record in rejected:
            file.write(json.dumps(record) + "\n")

    return {
        "processed": processed,
        "valid_customers": len(customers),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform MySQL CDC customers into Silver")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_customers(args.input, args.output, args.rejected)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
