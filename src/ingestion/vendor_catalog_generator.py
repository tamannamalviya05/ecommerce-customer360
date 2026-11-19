"""Generate synthetic vendor catalog data for the E-commerce project."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_OUTPUT_FILE = Path("data/vendor/vendor_catalog.json")

CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home",
    "Beauty",
    "Sports",
)

BRANDS = (
    "Zenith",
    "Aura",
    "Nova",
    "Prime",
    "Orbit",
)


def generate_vendor_catalog(count: int = 5000, seed: int = 42) -> list[dict]:
    """Generate reproducible synthetic vendor products."""
    if count < 1:
        raise ValueError("count must be at least 1")

    rng = random.Random(seed)
    base_time = datetime(2026, 11, 1, tzinfo=timezone.utc)

    records = []

    for number in range(1, count + 1):
        product = {
            "product_id": f"PROD{number:06d}",
            "sku": f"SKU{number:06d}",
            "product_name": f"Product {number:06d}",
            "category": rng.choice(CATEGORIES),
            "brand": rng.choice(BRANDS),
            "vendor_id": f"VEN{rng.randint(1, 20):03d}",
            "list_price": round(rng.uniform(100, 20000), 2),
            "currency": "INR",
            "stock_quantity": rng.randint(0, 1000),
            "updated_at": (base_time + timedelta(hours=rng.randint(0, 500))).isoformat(),
        }

        records.append(product)

    return records


def write_vendor_catalog(
    output_file: Path = DEFAULT_OUTPUT_FILE,
    count: int = 5000,
    seed: int = 42,
    overwrite: bool = False,
) -> dict:
    """Write generated products without overwriting existing data."""
    output_file = Path(output_file)

    if output_file.exists() and not overwrite:
        raise FileExistsError(
            f"File already exists: {output_file}. Use --overwrite only if replacement is intended."
        )

    records = generate_vendor_catalog(count=count, seed=seed)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)

    return {
        "records_generated": len(records),
        "output_file": str(output_file),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic vendor catalog products")

    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
    )

    args = parser.parse_args()

    result = write_vendor_catalog(
        output_file=args.output_file,
        count=args.count,
        seed=args.seed,
        overwrite=args.overwrite,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
