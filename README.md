# Cloud Logistics Lakehouse 🚀

## Overview
End-to-end data platform for logistics analytics using:
- Snowflake (data warehouse)
- dbt (transformations & data quality)
- Airflow (orchestration)
- Data Vault modeling (scalability & auditability)

## Architecture
Bronze → Silver → Gold → Data Vault

## Key Features
- Route performance KPIs
- Cost anomaly detection
- Data quality monitoring
- Data Vault (Hubs, Links, Satellites)
- Fully orchestrated pipeline via Airflow

## Tech Stack
Python, SQL, dbt, Snowflake, Airflow, Docker

## How to Run
1. Configure dbt profile (see profiles.yml.example)
2. Run:
   dbt run && dbt test
3. Start Airflow:
   docker compose up
