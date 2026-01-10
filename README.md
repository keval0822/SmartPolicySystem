# SmartPolicySystem

SmartPolicySystem is a data platform built on **Medallion Architecture** using **Azure Data Factory** and **Databricks**.  
It enables scalable ingestion, transformation, and analytics for policy data.

## Architecture
- **Bronze Layer**: Raw ingestion from source systems via ADF.
- **Silver Layer**: Cleaned and standardized data stored in Delta Lake.
- **Gold Layer**: Curated, business-ready datasets for reporting and analytics.

## Tech Stack
- Azure Data Factory (ADF) – Orchestration & ingestion
- Azure Databricks – Transformation & analytics
- Delta Lake – Reliable storage with ACID transactions

## Features
- Automated pipelines with incremental loads
- Layered data design for scalability
- Policy analytics dashboards
