"""Orchestrate dbt staging and quarantine validation."""

import os
import subprocess
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

ENVIRONMENTS = ("dev", "test", "prod")


def execute_dbt(arguments, environment):
    """Execute dbt against the selected environment."""
    subprocess.run(
        ["dbt", *arguments, "--target", environment],
        cwd="/opt/airflow/dbt/ecommerce",
        env={
            **os.environ,
            "DBT_ENV_SECRET_SNOWFLAKE_PASSWORD": os.environ["SNOWFLAKE_PASSWORD"],
            "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
            "DBT_TARGET_PATH": f"/tmp/dbt_target_{environment}",
            "DBT_LOG_PATH": f"/tmp/dbt_logs_{environment}",
        },
        check=True,
    )


@dag(
    dag_id="staging_quarantine",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "environment": Param(
            default="dev",
            type="string",
            enum=["dev", "test", "prod"],
        ),
        "confirm_prod": Param(
            default=False,
            type="boolean",
        ),
    },
    tags=["ecommerce", "staging", "quarantine", "snowflake"],
)
def staging_quarantine():

    @task
    def validate_environment(**context):
        """Validate the selected environment."""
        environment = context["params"]["environment"]
        confirm_prod = context["params"]["confirm_prod"]

        if environment not in ENVIRONMENTS:
            raise AirflowFailException(f"Unsupported environment: {environment}")

        if environment == "prod" and not confirm_prod:
            raise AirflowFailException("Production execution requires confirm_prod=true.")

        print(f"Selected dbt target: {environment}")

        return environment

    @task
    def run_staging_models(environment):
        """Run all dbt staging models."""
        execute_dbt(
            ["run", "--select", "path:models/staging"],
            environment,
        )

    @task
    def test_staging_models(environment):
        """Run dbt staging tests."""
        execute_dbt(
            ["test", "--select", "path:models/staging"],
            environment,
        )

    @task
    def run_quarantine_models(environment):
        """Create or refresh quarantine detection views."""
        execute_dbt(
            ["run", "--select", "path:models/quarantine"],
            environment,
        )

    @task
    def persist_quarantine_records(environment):
        """Persist invalid records in Snowflake."""
        macros = [
            "merge_payment_quarantine",
            "merge_ad_spend_quarantine",
            "merge_vendor_catalog_quarantine",
        ]

        for macro in macros:
            execute_dbt(
                ["run-operation", macro],
                environment,
            )

    environment = validate_environment()

    staging = run_staging_models(environment)
    staging_tests = test_staging_models(environment)
    quarantine = run_quarantine_models(environment)
    persist = persist_quarantine_records(environment)

    staging >> staging_tests >> quarantine >> persist


staging_quarantine()
