"""Generate synthetic e-commerce records in MySQL."""

from __future__ import annotations

import argparse
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import mysql.connector
from faker import Faker

CUSTOMER_COUNT = 2000
PRODUCT_COUNT = 200
ORDER_COUNT = 5000
ITEMS_PER_ORDER = 2

CUSTOMER_ID_START = 1000000
PRODUCT_ID_START = 1000000
ORDER_ID_START = 1000000
ORDER_ITEM_ID_START = 1000000

CATEGORIES = [
    "Electronics",
    "Fashion",
    "Home",
    "Beauty",
    "Sports",
    "Books",
    "Kitchen",
    "Accessories",
]

ORDER_STATUSES = [
    "delivered",
    "shipped",
    "processing",
    "cancelled",
    "returned",
]


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.environ["MYSQL_PASSWORD"],
        database="ecommerce",
    )


def money(value):
    return Decimal(str(value)).quantize(Decimal("0.01"))


def generate_data():
    fake = Faker("en_IN")
    Faker.seed(42)
    rng = random.Random(42)

    connection = get_connection()
    cursor = connection.cursor()

    try:
        # Protect existing data: do not overwrite generated IDs.
        for table, column, start, count in [
            ("customers", "customer_id", CUSTOMER_ID_START, CUSTOMER_COUNT),
            ("products", "product_id", PRODUCT_ID_START, PRODUCT_COUNT),
            ("orders", "order_id", ORDER_ID_START, ORDER_COUNT),
            (
                "order_items",
                "order_item_id",
                ORDER_ITEM_ID_START,
                ORDER_COUNT * ITEMS_PER_ORDER,
            ),
        ]:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {column} >= %s AND {column} < %s",
                (start, start + count),
            )
            if cursor.fetchone()[0]:
                raise RuntimeError(
                    f"Generated ID range already occupied in {table}. "
                    "No records have been inserted."
                )

        print("Generating customers...")
        customers = []

        for index in range(CUSTOMER_COUNT):
            customer_id = CUSTOMER_ID_START + index
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = f"customer{customer_id}@example.com"

            created_at = fake.date_time_between(
                start_date="-2y",
                end_date="-30d",
                tzinfo=timezone.utc,
            ).replace(tzinfo=None)

            customers.append(
                (
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    fake.city(),
                    "India",
                    created_at,
                    created_at,
                )
            )

        cursor.executemany(
            """
            INSERT INTO customers (
                customer_id, first_name, last_name, email,
                city, country, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            customers,
        )
        connection.commit()

        print("Generating products...")
        products = []

        for index in range(PRODUCT_COUNT):
            product_id = PRODUCT_ID_START + index
            category = rng.choice(CATEGORIES)
            price = money(rng.uniform(199, 29999))

            products.append(
                (
                    product_id,
                    f"{category} Product {index + 1:03d}",
                    category,
                    price,
                    rng.randint(50, 1000),
                )
            )

        cursor.executemany(
            """
            INSERT INTO products (
                product_id, product_name, category,
                price, stock_quantity
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            products,
        )
        connection.commit()

        print("Generating orders and order items...")

        for batch_start in range(0, ORDER_COUNT, 250):
            orders = []
            order_items = []

            for index in range(
                batch_start,
                min(batch_start + 250, ORDER_COUNT),
            ):
                order_id = ORDER_ID_START + index
                customer_id = CUSTOMER_ID_START + rng.randrange(CUSTOMER_COUNT)
                status = rng.choice(ORDER_STATUSES)

                created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
                    days=rng.randint(0, 180)
                )

                total = Decimal("0.00")

                for item_index in range(ITEMS_PER_ORDER):
                    item_id = ORDER_ITEM_ID_START + index * ITEMS_PER_ORDER + item_index

                    product = rng.choice(products)
                    product_id = product[0]
                    unit_price = product[3]
                    quantity = rng.randint(1, 4)

                    subtotal = unit_price * quantity
                    discount = money(subtotal * Decimal(str(rng.choice([0, 0.05, 0.10]))))
                    taxable_amount = subtotal - discount
                    tax = money(taxable_amount * Decimal("0.05"))
                    shipping = money(rng.choice([0, 0, 49, 99]))
                    line_total = subtotal - discount + tax + shipping

                    total += line_total

                    order_items.append(
                        (
                            item_id,
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            discount,
                            tax,
                            shipping,
                            line_total,
                            created_at,
                            created_at,
                        )
                    )

                orders.append(
                    (
                        order_id,
                        customer_id,
                        status,
                        total,
                        created_at,
                        created_at,
                    )
                )

            cursor.executemany(
                """
                INSERT INTO orders (
                    order_id, customer_id, order_status,
                    total_amount, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                orders,
            )

            cursor.executemany(
                """
                INSERT INTO order_items (
                    order_item_id, order_id, product_id,
                    quantity, unit_price, discount_amount,
                    tax_amount, shipping_amount, line_total,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                order_items,
            )

            connection.commit()
            print(f"Completed {min(batch_start + 250, ORDER_COUNT)} of {ORDER_COUNT} orders")

        print("Synthetic MySQL data generation completed.")

    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually insert generated records into MySQL",
    )
    args = parser.parse_args()

    if not args.confirm:
        print("Planned: 2000 customers, 200 products, 5000 orders, 10000 order items.")
        print("Use --confirm to insert records.")
    else:
        generate_data()
