"""Orchestrate dbt staging and quarantine validation."""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="staging_quarantine",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "staging", "quarantine", "snowflake"],
)
def staging_quarantine():

    @task
    def run_staging_models():
        """Run all dbt staging models."""
        import os
        import subprocess

        subprocess.run(
            ["dbt", "run", "--select", "path:models/staging"],
            cwd="/opt/airflow/dbt/ecommerce",
            env={
                **os.environ,
                "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
                "DBT_TARGET_PATH": "/tmp/dbt_target",
                "DBT_LOG_PATH": "/tmp/dbt_logs",
            },
            check=True,
        )

    @task
    def test_staging_models():
        """Run dbt staging tests."""
        import os
        import subprocess

        subprocess.run(
            ["dbt", "test", "--select", "path:models/staging"],
            cwd="/opt/airflow/dbt/ecommerce",
            env={
                **os.environ,
                "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
                "DBT_TARGET_PATH": "/tmp/dbt_target",
                "DBT_LOG_PATH": "/tmp/dbt_logs",
            },
            check=True,
        )

    @task
    def run_quarantine_models():
        """Create or refresh quarantine detection views."""
        import os
        import subprocess

        subprocess.run(
            ["dbt", "run", "--select", "path:models/quarantine"],
            cwd="/opt/airflow/dbt/ecommerce",
            env={
                **os.environ,
                "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
                "DBT_TARGET_PATH": "/tmp/dbt_target",
                "DBT_LOG_PATH": "/tmp/dbt_logs",
            },
            check=True,
        )

    @task
    def persist_quarantine_records():
        """Persist invalid records in Snowflake."""
        import os
        import subprocess

        macros = [
            "merge_payment_quarantine",
            "merge_ad_spend_quarantine",
            "merge_vendor_catalog_quarantine",
        ]

        for macro in macros:
            subprocess.run(
                ["dbt", "run-operation", macro],
                cwd="/opt/airflow/dbt/ecommerce",
                env={
                    **os.environ,
                    "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
                    "DBT_TARGET_PATH": "/tmp/dbt_target",
                    "DBT_LOG_PATH": "/tmp/dbt_logs",
                },
                check=True,
            )

    staging = run_staging_models()
    staging_tests = test_staging_models()
    quarantine = run_quarantine_models()
    persist = persist_quarantine_records()

    staging >> staging_tests >> quarantine >> persist


staging_quarantine()
