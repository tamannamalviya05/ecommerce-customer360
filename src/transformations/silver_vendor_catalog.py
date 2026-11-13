import argparse
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_INPUT = Path("data/bronze/vendor_catalog/raw_vendor_catalog.jsonl")
DEFAULT_OUTPUT = Path("data/silver/vendor_catalog.jsonl")
DEFAULT_REJECTED = Path("data/silver/rejected/vendor_catalog_rejected.jsonl")


def validate_price(value):
    """Validate a non-negative price while preserving decimal precision."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid list_price")

    try:
        price = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid list_price") from exc

    if not price.is_finite():
        raise ValueError("Invalid list_price")

    if price < 0:
        raise ValueError("list_price cannot be negative")

    return format(price, "f")


def validate_timestamp(value):
    """Validate an ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing updated_at")

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid updated_at") from exc

    return value


def require_text(value, field):
    """Validate and clean a required text field."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing {field}")

    return value.strip()


def transform_vendor_product(record):
    """Clean and validate one Bronze vendor product."""
    if not isinstance(record, dict):
        raise ValueError("Invalid Bronze record")

    product = record.get("raw_payload")

    if not isinstance(product, dict):
        raise ValueError("Missing vendor product payload")

    product_id = require_text(product.get("product_id"), "product_id")
    sku = require_text(product.get("sku"), "sku")
    product_name = require_text(product.get("product_name"), "product_name")
    category = require_text(product.get("category"), "category")
    brand = require_text(product.get("brand"), "brand")
    vendor_id = require_text(product.get("vendor_id"), "vendor_id")

    currency = require_text(product.get("currency"), "currency").upper()

    if currency != "INR":
        raise ValueError("Invalid currency")

    stock_quantity = product.get("stock_quantity")

    if isinstance(stock_quantity, bool) or not isinstance(stock_quantity, int):
        raise ValueError("Invalid stock_quantity")

    if stock_quantity < 0:
        raise ValueError("stock_quantity cannot be negative")

    return {
        "product_id": product_id,
        "sku": sku,
        "product_name": product_name,
        "category": category,
        "brand": brand,
        "vendor_id": vendor_id,
        "list_price": validate_price(product.get("list_price")),
        "currency": currency,
        "stock_quantity": stock_quantity,
        "updated_at": validate_timestamp(product.get("updated_at")),
        "_source_system": record.get("_source_system"),
        "_ingested_at": record.get("_ingested_at"),
    }


def process_vendor_catalog(input_path, output_path, rejected_path):
    """Build Silver vendor catalog and quarantine invalid rows."""
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
                bronze_record = json.loads(line)
                product = transform_vendor_product(bronze_record)

                # Keep the last valid occurrence of each product ID.
                products[product["product_id"]] = product

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
        "valid_vendor_products": len(products),
        "rejected": len(rejected),
    }


def main():
    parser = argparse.ArgumentParser(description="Transform Bronze vendor catalog into Silver")

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rejected", type=Path, default=DEFAULT_REJECTED)

    args = parser.parse_args()

    result = process_vendor_catalog(
        input_path=args.input, output_path=args.output, rejected_path=args.rejected
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
