# Orders Backfilling Pipeline using Airflow + Dataproc

## Project Overview

This project demonstrates a historical orders backfilling pipeline built using Apache Airflow, Google Cloud Dataproc, PySpark, and Google Cloud Storage (GCS).

The pipeline dynamically processes multiple historical order files stored in GCS and performs distributed data processing using Dataproc clusters.

Processed output files are written back into GCS after successful execution.

---

# Architecture

GCS (Input CSV Files)
        ↓
Apache Airflow DAG
        ↓
Dataproc PySpark Job
        ↓
PySpark Transformations
        ↓
GCS Output Folder

---

# Technologies Used

- Apache Airflow
- Google Cloud Platform (GCP)
- Google Cloud Storage (GCS)
- Google Cloud Dataproc
- PySpark
- Python
- Docker

---

# Project Workflow

1. Historical CSV order files are uploaded into GCS.
2. Airflow DAG gets triggered manually.
3. DAG extracts execution date.
4. Dataproc job is submitted dynamically.
5. PySpark processes historical order data.
6. Transformed output files are written back into GCS.
7. Airflow tracks Dataproc job metadata using XCom.

---

# DAG Workflow

The DAG contains the following tasks:

## 1. get_execution_date

- Fetches execution date dynamically
- Helps process historical/backfill data

## 2. submit_pyspark_job

- Submits PySpark job to Dataproc
- Executes distributed data transformations
- Stores processed output in GCS

---

# Folder Structure

```text
airflow-exercise-2/
│
├── dags/
│   └── orders_backfilling_dag.py
│
├── spark_job/
│   └── orders_data_process.py
│
├── data/
│   ├── orders_20250913.csv
│   ├── orders_20250914.csv
│   └── orders_20250915.csv
│
├── screenshots/
│   ├── dag_success.png
│   ├── gcs_output.png
│   ├── dataproc_job_logs.png
│   └── xcom_job_metadata.png
│
└── README.md