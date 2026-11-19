"""Orchestrate E-commerce Bronze ingestion into Snowflake."""

from datetime import datetime

from airflow.decorators import dag, task

SOURCES = [
    "payments",
    "ad_spend",
    "clickstream",
    "mysql_cdc_customers",
    "mysql_cdc_orders",
    "mysql_cdc_order_items",
    "vendor_catalog",
    "fx_rates",
]


@dag(
    dag_id="bronze_ingestion",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "bronze", "snowflake"],
)
def bronze_ingestion():

    @task
    def ingest_payment_data():
        from src.ingestion.payment_api_ingestor import ingest_payments

        result = ingest_payments(
            base_url="http://host.docker.internal:8000",
            page_size=1000,
        )

        print(f"Payments ingestion result: {result}")
        return result

    @task
    def ingest_ad_spend_data():
        from src.ingestion.ad_spend_api_ingestor import ingest_ad_spend

        result = ingest_ad_spend(
            base_url="http://host.docker.internal:8001",
            page_size=1000,
        )

        print(f"Ad Spend ingestion result: {result}")
        return result

    @task
    def ingest_vendor_catalog_data(**context):
        from datetime import date
        from pathlib import Path

        from src.ingestion.vendor_catalog_ingestor import ingest_vendor_catalog

        dag_run = context["dag_run"]
        feed_date_str = (dag_run.conf or {}).get("vendor_feed_date")

        if not feed_date_str:
            from airflow.exceptions import AirflowSkipException

            raise AirflowSkipException(
                "Vendor Catalog ingestion skipped: vendor_feed_date was not provided."
            )

        feed_date = date.fromisoformat(feed_date_str)

        result = ingest_vendor_catalog(
            input_file=Path("data/vendor/vendor_catalog.json"),
            feed_date=feed_date,
        )

        print(f"Vendor Catalog ingestion result: {result}")
        return result

    @task
    def ingest_fx_rates_data():
        from src.ingestion.fx_rates_ingestor import ingest_fx_rates

        result = ingest_fx_rates()

        print(f"FX Rates ingestion result: {result}")
        return result

    @task
    def check_bronze_file(source):
        from src.ingestion.snowflake_bronze_loader import TABLES

        file_path, table = TABLES[source]

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        print(f"Validated: {source}")
        print(f"Target: C360_DEV.BRONZE.{table}")

        return source

    @task
    def load_snowflake_bronze(source):
        from src.ingestion.snowflake_bronze_loader import load_bronze

        load_bronze(source=source, dry_run=False)

    payments_ingestion = ingest_payment_data()
    ad_spend_ingestion = ingest_ad_spend_data()
    vendor_ingestion = ingest_vendor_catalog_data()
    fx_rates_ingestion = ingest_fx_rates_data()

    source_dependencies = {
        "payments": payments_ingestion,
        "ad_spend": ad_spend_ingestion,
        "vendor_catalog": vendor_ingestion,
        "fx_rates": fx_rates_ingestion,
    }

    for source in SOURCES:
        validated = check_bronze_file.override(task_id=f"check_{source}")(source)

        load_snowflake_bronze.override(task_id=f"load_{source}")(validated)

        if source in source_dependencies:
            source_dependencies[source] >> validated


bronze_ingestion()
