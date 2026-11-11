# E-commerce Customer 360 & Personalization Engine

An end-to-end data engineering project that builds a unified customer view from e-commerce transactions, payments, clickstream events, advertising data, and vendor feeds.

## Project Objectives

- Ingest batch and streaming data from multiple sources.
- Store raw data in a Bronze layer.
- Validate data and quarantine invalid records.
- Transform data into Silver and Gold layers using dbt.
- Build customer analytics and personalization recommendations.
- Orchestrate pipelines using Apache Airflow.
- Visualize business KPIs using Streamlit.

## Technology Stack

- Python
- Apache Airflow
- Docker
- MySQL
- Apache Kafka
- Debezium
- Snowflake
- dbt
- Streamlit
- GitHub Actions

## Architecture

Sources → Ingestion → Bronze → Data Quality → Silver → Gold → Streamlit Dashboard

## Project Structure

E-commerce/
├── .github/workflows/
├── airflow/
│   ├── dags/
│   └── config/
├── dbt/
├── docker/
├── docs/
├── scripts/
├── src/
│   ├── ingestion/
│   ├── quality/
│   └── producers/
├── streamlit/
├── tests/
├── .gitignore
└── README.md