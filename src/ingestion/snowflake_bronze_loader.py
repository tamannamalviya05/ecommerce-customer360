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
        Path("data/bronze/clickstream/clickstream_events.jsonl"),
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


def load_bronze(source):
    file_path, table = TABLES[source]
    file_path = file_path.resolve()

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    with file_path.open(encoding="utf-8") as file:
        for line_number, line in enumerate(file, 1):
            if line.strip():
                json.loads(line)

    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database="C360_DEV",
        schema="BRONZE",
    )

    try:
        cursor = connection.cursor()
        try:
            # Use a separate stage prefix for each source.
            stage_path = f"@C360_DEV.BRONZE.BRONZE_UPLOAD_STAGE/{source}"

            put_sql = (
                f"PUT 'file://{file_path.as_posix()}' "
                f"{stage_path} "
                "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )
            cursor.execute(put_sql)

            copy_sql = f"""
                COPY INTO C360_DEV.BRONZE.{table}
                    (
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
                    FROM {stage_path}
                )
                FILE_FORMAT = (
                    FORMAT_NAME =
                    'C360_DEV.BRONZE.BRONZE_JSONL_FORMAT'
                )
                ON_ERROR = ABORT_STATEMENT
            """

            cursor.execute(copy_sql)
            for result in cursor.fetchall():
                print(result)

            cursor.execute(f"SELECT COUNT(*) FROM C360_DEV.BRONZE.{table}")
            print(f"{table} total rows:", cursor.fetchone()[0])

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
    args = parser.parse_args()
    load_bronze(args.source)
