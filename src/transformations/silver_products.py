import argparse
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/mysql_cdc/ecommerce_ecommerce_products.jsonl")
DEFAULT_OUTPUT = Path("data/silver/products.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/products_rejected.jsonl")


def validate_timestamp(value):
    """Validate an ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing or invalid timestamp")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid timestamp format") from exc

    return value


def validate_price(value):
    """Validate a product price without losing decimal precision."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid product price")

    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid product price") from exc

    if not price.is_finite():
        raise ValueError("Invalid product price")

    if price < 0:
        raise ValueError("Product price cannot be negative")

    return format(price, "f")


def transform_product(record):
    """Transform one Debezium product CDC event."""
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

    product = envelope.get("before") if operation == "d" else envelope.get("after")

    if not isinstance(product, dict):
        raise ValueError("Missing product data")

    product_id = product.get("product_id")

    if isinstance(product_id, bool) or not isinstance(product_id, int):
        raise ValueError("Invalid product_id")

    if product_id <= 0:
        raise ValueError("product_id must be positive")

    if operation == "d":
        return {
            "product_id": product_id,
            "_deleted": True,
            "_cdc_operation": operation,
            "_ingested_at": record.get("_ingested_at"),
        }

    product_name = product.get("product_name")
    category = product.get("category")

    if not isinstance(product_name, str) or not product_name.strip():
        raise ValueError("Missing product_name")

    if not isinstance(category, str) or not category.strip():
        raise ValueError("Missing category")

    stock_quantity = product.get("stock_quantity")

    if isinstance(stock_quantity, bool) or not isinstance(stock_quantity, int):
        raise ValueError("Invalid stock_quantity")

    if stock_quantity < 0:
        raise ValueError("stock_quantity cannot be negative")

    return {
        "product_id": product_id,
        "product_name": product_name.strip(),
        "category": category.strip(),
        "price": validate_price(product.get("price")),
        "stock_quantity": stock_quantity,
        "updated_at": validate_timestamp(product.get("updated_at")),
        "_deleted": False,
        "_cdc_operation": operation,
        "_ingested_at": record.get("_ingested_at"),
    }


def process_products(input_path, output_path, rejected_path):
    """Build the current product snapshot from ordered CDC events."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    rejected_path = Path(rejected_path)

    products = {}
    rejected = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            processed += 1

            try:
                record = json.loads(line)
                product = transform_product(record)
                product_id = product["product_id"]

                if product["_deleted"]:
                    products.pop(product_id, None)
                else:
                    products[product_id] = product

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
        for product in products.values():
            file.write(json.dumps(product) + "\n")

    with rejected_path.open("w", encoding="utf-8") as file:
        for record in rejected:
            file.write(json.dumps(record) + "\n")

    return {
        "processed": processed,
        "valid_products": len(products),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform MySQL CDC products into Silver")

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_products(args.input, args.output, args.rejected)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
