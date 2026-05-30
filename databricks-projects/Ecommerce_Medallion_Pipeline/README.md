# Ecommerce Medallion Pipeline using Databricks

## Project Overview

This project demonstrates an end-to-end enterprise-style Medallion Architecture pipeline built using Databricks and Delta Lake.

The pipeline processes ecommerce datasets through Bronze, Silver, and Gold layers with workflow orchestration, data validation, JSON trigger-based execution, and analytics transformations.

---

## Architecture

Raw CSV Files
→ Bronze / Stage Layer
→ Silver Layer
→ Gold Analytics Layer

Workflow Orchestration:
JSON Trigger Files
→ Master Notebook
→ Databricks Workflow

---

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Databricks Workflows
- Unity Catalog Volumes
- JSON Trigger Files
- Medallion Architecture

---

## Features Implemented

### Bronze / Stage Layer
- Incremental file ingestion
- Schema enforcement
- Data validation
- Error handling
- Archive management

### Silver Layer
- Data cleansing
- Business transformations
- Multi-table joins
- Curated business dataset

### Gold Layer
- KPI generation
- Revenue analytics
- Customer analytics
- Aggregated reporting tables

### Orchestration
- JSON trigger-based execution
- Master orchestration notebook
- Databricks Workflow automation

---

## Pipeline Flow

1. Trigger JSON file validated
2. Bronze notebooks ingest source files
3. Validation notebook performs quality checks
4. Silver layer creates integrated business dataset
5. Gold layer generates analytics-ready KPIs
6. Databricks Workflow orchestrates complete execution

---

## Sample Datasets

- Orders
- Customers
- Products
- Inventory
- Shipping

---

## Screenshots

Project screenshots available inside the `screenshots` folder.

---

## Future Enhancements

- SCD Type 2 implementation
- Streaming ingestion
- CI/CD deployment
- Automated monitoring & alerting

---

## Author

Aishwarya K