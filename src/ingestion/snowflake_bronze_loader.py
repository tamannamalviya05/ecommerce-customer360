"""Load E-commerce Bronze JSONL records into Snowflake safely."""

import argparse
import json
import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(".env.snowflake")

TABLES = {
    "payments": (
        Path("data/bronze/payments/raw_payments.jsonl"),
        "RAW_PAYMENTS",
    ),
    "ad_spend": (
        Path("data/bronze/ad_spend/raw_ad_spend.jsonl"),
        "RAW_AD_SPEND",
    ),
    "clickstream": (
        Path("data/bronze/clickstream/clickstream_20000.jsonl"),
        "RAW_CLICKSTREAM",
    ),
    "mysql_cdc_customers": (
        Path("data/bronze/mysql_cdc/ecommerce_ecommerce_customers.jsonl"),
        "RAW_CDC_CUSTOMERS",
    ),
    "mysql_cdc_orders": (
        Path("data/bronze/mysql_cdc/ecommerce_ecommerce_orders.jsonl"),
        "RAW_CDC_ORDERS",
    ),
    "mysql_cdc_order_items": (
        Path("data/bronze/mysql_cdc/ecommerce_ecommerce_order_items.jsonl"),
        "RAW_CDC_ORDER_ITEMS",
    ),
    "vendor_catalog": (
        Path("data/bronze/vendor_catalog/raw_vendor_catalog.jsonl"),
        "RAW_VENDOR_CATALOG",
    ),
    "fx_rates": (
        Path("data/bronze/fx_rates/raw_fx_rates.jsonl"),
        "RAW_FX_RATES",
    ),
}

KEYS = {
    "payments": "RAW_PAYLOAD:payment_id::STRING",
    "ad_spend": "RAW_PAYLOAD:spend_id::STRING",
    "clickstream": "RAW_PAYLOAD:event_id::STRING",
    "mysql_cdc_customers": "_BATCH_ID",
    "mysql_cdc_orders": "_BATCH_ID",
    "mysql_cdc_order_items": "_BATCH_ID",
    "vendor_catalog": "TO_JSON(RAW_PAYLOAD)",
    "fx_rates": (
        "TO_JSON(ARRAY_CONSTRUCT("
        "RAW_PAYLOAD:rate_date::STRING, "
        "RAW_PAYLOAD:base_currency::STRING, "
        "RAW_PAYLOAD:quote_currency::STRING))"
    ),
}


def validate_file(path):
    """Validate the local JSONL file."""
    count = 0

    with path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_number}") from exc

            if not isinstance(record, dict):
                raise ValueError(f"Expected JSON object at line {line_number}")

            if not isinstance(record.get("raw_payload"), dict):
                raise ValueError(f"Missing raw_payload at line {line_number}")

            count += 1

    return count


def get_key_expression(source, alias):
    """Return the deduplication expression for a table alias."""
    expression = KEYS[source]

    expression = expression.replace("RAW_PAYLOAD", f"{alias}.RAW_PAYLOAD")

    expression = expression.replace("_BATCH_ID", f"{alias}._BATCH_ID")

    return expression


def load_bronze(source, dry_run=False):
    """Stage records and insert only previously unseen keys."""
    if source not in TABLES:
        raise ValueError(f"Unsupported source: {source}")

    path, table = TABLES[source]
    path = path.resolve()

    if not path.is_file():
        raise FileNotFoundError(path)

    local_count = validate_file(path)

    print(f"Source: {source}")
    print(f"Validated records: {local_count}")

    if local_count == 0:
        print("No records to load.")
        return

    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database="C360_DEV",
        schema="BRONZE",
    )

    target = f"C360_DEV.BRONZE.{table}"
    stage = f"@C360_DEV.BRONZE.BRONZE_UPLOAD_STAGE/{source}/"
    staged_filename = f"{path.name}.gz"
    staging_key = get_key_expression(source, "s")
    target_key = get_key_expression(source, "t")

    try:
        cursor = connection.cursor()

        try:
            cursor.execute(
                """
                CREATE TEMPORARY TABLE BRONZE_LOAD_STAGING (
                    RAW_PAYLOAD VARIANT,
                    _INGESTED_AT TIMESTAMP_TZ,
                    _SOURCE_SYSTEM VARCHAR,
                    _BATCH_ID VARCHAR
                )
                """
            )

            put_sql = f"PUT 'file://{path.as_posix()}' {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            cursor.execute(put_sql)

            copy_sql = f"""
                COPY INTO BRONZE_LOAD_STAGING (
                    RAW_PAYLOAD,
                    _INGESTED_AT,
                    _SOURCE_SYSTEM,
                    _BATCH_ID
                )
                FROM (
                    SELECT
                       $1:raw_payload,
                       TRY_TO_TIMESTAMP_TZ(
                       $1:_ingested_at::STRING
                      ),
                      $1:_source_system::STRING,
                      $1:_batch_id::STRING
                    FROM {stage}
                )
                FILES = ('{staged_filename}')
                FILE_FORMAT = (
                    FORMAT_NAME =
                    'C360_DEV.BRONZE.BRONZE_JSONL_FORMAT'
                )
                ON_ERROR = ABORT_STATEMENT
            """
            cursor.execute(copy_sql)

            cursor.execute("SELECT COUNT(*) FROM BRONZE_LOAD_STAGING")
            staged_count = cursor.fetchone()[0]

            if staged_count != local_count:
                raise RuntimeError(f"Staging count mismatch: {staged_count} != {local_count}")

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM BRONZE_LOAD_STAGING s
                WHERE {staging_key} IS NULL
                """
            )
            missing_keys = cursor.fetchone()[0]

            if missing_keys:
                raise ValueError(f"{missing_keys} records have missing deduplication keys")

            # Check how many distinct staged keys are new.
            new_records_sql = f"""
                SELECT COUNT(*)
                FROM (
                    SELECT
                        {staging_key} AS RECORD_KEY
                    FROM BRONZE_LOAD_STAGING s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY RECORD_KEY
                        ORDER BY s._INGESTED_AT
                    ) = 1
                ) incoming
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {target} t
                    WHERE {target_key} = incoming.RECORD_KEY
                )
            """

            cursor.execute(new_records_sql)
            new_records = cursor.fetchone()[0]

            print(f"Staged records: {staged_count}")
            print(f"New records: {new_records}")
            print(f"Existing or repeated records: {staged_count - new_records}")

            if dry_run:
                print("DRY RUN: No target records inserted.")
                return

            # Insert distinct, previously unseen records.
            merge_sql = f"""
                MERGE INTO {target} t
                USING (
                    SELECT
                        s.RAW_PAYLOAD,
                        s._INGESTED_AT,
                        s._SOURCE_SYSTEM,
                        s._BATCH_ID,
                        {staging_key} AS RECORD_KEY
                    FROM BRONZE_LOAD_STAGING s
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY RECORD_KEY
                        ORDER BY s._INGESTED_AT
                    ) = 1
                ) incoming
                ON {target_key} = incoming.RECORD_KEY
                WHEN NOT MATCHED THEN INSERT (
                    RAW_PAYLOAD,
                    _INGESTED_AT,
                    _SOURCE_SYSTEM,
                    _BATCH_ID
                )
                VALUES (
                    incoming.RAW_PAYLOAD,
                    incoming._INGESTED_AT,
                    incoming._SOURCE_SYSTEM,
                    incoming._BATCH_ID
                )
            """

            try:
                cursor.execute("BEGIN")
                cursor.execute(merge_sql)
                result = cursor.fetchone()
                connection.commit()
            except Exception:
                connection.rollback()
                raise

            inserted = result[0] if result else 0

            print(f"Inserted records: {inserted}")
            print(f"Skipped records: {staged_count - inserted}")

            cursor.execute(f"SELECT COUNT(*) FROM {target}")
            total = cursor.fetchone()[0]

            print(f"{table} total rows: {total}")

        finally:
            cursor.close()

    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source",
        choices=TABLES.keys(),
        required=True,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and compare without inserting target records",
    )

    args = parser.parse_args()

    load_bronze(
        source=args.source,
        dry_run=args.dry_run,
    )
