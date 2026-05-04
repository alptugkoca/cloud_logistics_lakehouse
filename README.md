# Cloud Logistics Lakehouse (AWS + PySpark + Delta Lake)

## Overview
This project implements a cloud-based data platform for logistics cost and network analytics using a Medallion Architecture (Bronze → Silver → Gold).

Built with PySpark, Delta Lake, and AWS S3, the platform transforms raw logistics, shipment, and event data into structured, analytics-ready datasets for cost optimization, operational monitoring, and KPI-driven decision-making.

---

## Architecture

S3 (Raw Data)
   ↓
Bronze Layer (Ingestion)
   ↓
Silver Layer (Cleaning & Transformation)
   ↓
Gold Layer (KPIs & Analytics)

---

## Tech Stack

- Processing: PySpark  
- Storage: AWS S3  
- Format: Delta Lake  
- Architecture: Medallion (Bronze/Silver/Gold)  
- Language: Python  
- Environment: Local + Cloud (Config-driven)  
- Tools: VS Code, Git  

---

## Project Structure

cloud-logistics-lakehouse/
│
├── data/
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── common/
│
├── docs/
├── notebooks/
└── README.md

---

## Data Flow

### Bronze Layer
- Ingests raw CSV data from S3  
- Stores data in Delta format  

### Silver Layer
- Cleans and standardizes data  
- Applies schema casting  
- Feature engineering  

### Gold Layer
- Logistics KPIs  
- Funnel KPIs  
- Incident KPIs  

---

## Cloud Deployment

- Amazon S3  
- Spark S3A connector  
- AWS CLI authentication  

---

## Run

Local:
export APP_ENV=dev
python -m src.bronze.batch_ingest

AWS:
export APP_ENV=aws
python -m src.bronze.batch_ingest
python -m src.bronze.events_ingest
python -m src.silver.transform_batch
python -m src.silver.transform_events
python -m src.gold.logistics_kpi
python -m src.gold.funnel_kpi
python -m src.gold.incidents_kpi

---
## Streaming

The project includes a streaming-ready ingestion layer built with Spark Structured Streaming.

### Streaming Flow
JSON Event Producer
   ↓
Streaming Input Folder
   ↓
Spark Structured Streaming
   ↓
Bronze Delta Tables

### Streaming Outputs
- bronze/streaming_events
- bronze/streaming_incidents

### Use Cases
- Near real-time event ingestion
- Incremental logistics incident tracking
- Live-ready KPI pipeline extension

## Business Scenarios

### Route Optimization
Identify high-cost routes based on cost per km and shipment patterns to optimize transport strategies.

### Incident Risk Monitoring
Track logistics incidents by type and severity to improve operational reliability.

### Funnel Performance Analysis
Analyze user behavior across logistics events to identify drop-offs and improve conversion.

## Author
Alptuğ Koca

## 🏗️ Architecture Overview

This project implements a modern end-to-end data platform using:

- **Apache Airflow** for orchestration  
- **Snowflake** as the data warehouse  
- **dbt** for transformation and testing  
- **Data Vault modeling** for scalability and auditability  
- **GitHub Actions** for CI/CD  
- **Terraform & Bicep (skeleton)** for Infrastructure-as-Code  

---

## 🔄 Data Flow

Sources → Airflow → Snowflake  
          ↓  
          dbt  
    (Staging → Data Vault → Marts)  
          ↓  
     Data Quality + Alerts  
          ↓  
       BI / Consumers  

---

## 🧱 Data Layers

### 1. Staging Layer
- Cleans and standardizes raw data  
- Serves as the foundation for transformations  

### 2. Data Vault Layer
- **Hubs** → Core business keys (route, shipment, transport mode)  
- **Links** → Relationships between entities  
- **Satellites** → Descriptive attributes and historical tracking  

### 3. Mart Layer
- Business-ready datasets  
- KPIs such as:
  - Cost performance  
  - Route efficiency  
  - Anomaly detection  

---

## ✅ Data Quality

- dbt tests (`not_null`, `unique`)  
- Data quality monitoring views  
- Alert generation for anomalies  

---

## ⚙️ Orchestration

Airflow DAG executes:

- Data ingestion  
- Transformation pipelines  
- dbt run & dbt test  
- Data quality checks  
- Alert creation  

---

## 🚀 CI/CD

GitHub Actions pipeline:

- Runs `dbt debug`  
- Executes `dbt run`  
- Executes `dbt test`  
- Acts as a quality gate before deployment  

---

## 🏗️ Infrastructure (Conceptual)

- **Terraform** → Snowflake resources (DB, schema, warehouse)  
- **Bicep** → Azure storage (Raw/Bronze/Silver/Gold layers)  

---

## 🎯 Key Highlights

- End-to-end pipeline (batch + transformations + analytics)  
- Data Vault implementation for scalability  
- Strong data quality layer  
- Production-oriented design (CI/CD + IaC thinking)  

