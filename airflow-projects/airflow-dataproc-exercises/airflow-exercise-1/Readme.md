# Employee Data Analysis Pipeline using Airflow + Dataproc

## Project Overview

This project demonstrates an end-to-end batch data processing pipeline using Apache Airflow, Google Cloud Dataproc, PySpark, and Google Cloud Storage (GCS).

The pipeline processes employee and department datasets stored in GCS using PySpark jobs running on Dataproc clusters.

Processed output data is generated after successful Spark transformations.

---

# Architecture

GCS (CSV Input Files)
        ↓
Apache Airflow DAG
        ↓
Dataproc Cluster
        ↓
PySpark Processing
        ↓
Processed Output to GCS

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

1. Employee and department CSV files are uploaded into GCS.
2. Airflow DAG is triggered manually.
3. Dataproc cluster is created dynamically.
4. PySpark job is submitted to Dataproc.
5. Employee data transformations are performed using Spark.
6. Processed output files are generated in GCS.
7. Dataproc cluster is deleted after successful execution.

---

# DAG Workflow

The DAG contains the following tasks:

## 1. create_dataproc_cluster

- Creates Dataproc cluster dynamically
- Provisions master and worker nodes

## 2. submit_pyspark_job_on_dataproc

- Executes PySpark transformation job
- Reads employee datasets from GCS
- Performs Spark transformations

## 3. delete_dataproc_cluster

- Deletes Dataproc cluster after execution
- Helps optimize cloud resource usage

---

# Folder Structure

```text
airflow-exercise-1/
│
├── dags/
│   └── employee_data_analysis_dag.py
│
├── spark_job/
│   └── employee_data_processing.py
│
├── data/
│   ├── employee.csv
│   └── department.csv
│
├── screenshots/
│   ├── dag_success.png
│   ├── dataproc_success.png
│   └── gcs_output.png
│
└── README.md