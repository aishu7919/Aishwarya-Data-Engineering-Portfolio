# Healthcare Data Pipeline using Databricks DLT

## Project Overview

This project demonstrates an end-to-end Healthcare Data Pipeline built using Databricks Delta Live Tables (DLT) and PySpark.

The pipeline follows the Medallion Architecture (Bronze, Silver, and Gold layers) to process healthcare patient data and generate analytical insights.

## Architecture

```text
Bronze Layer
├── daily_patients
└── diagnostic_mapping

        ↓

Silver Layer
└── processed_patient_data

        ↓

Gold Layer
├── patient_statistics_by_diagnosis
├── patient_statistics_by_gender
└── patient_statistics_by_admission_date
```

## Technologies Used

* Databricks
* Delta Live Tables (DLT)
* PySpark
* Delta Lake
* Python

## Data Sources

### daily_patients

Contains patient information such as:

* Patient ID
* Name
* Age
* Gender
* Address
* Contact Number
* Admission Date
* Diagnosis Code

### diagnostic_mapping

Contains diagnosis reference information:

* Diagnosis Code
* Diagnosis Description

## Pipeline Layers

### Bronze Layer

#### daily_patients

Loads raw patient data from the bronze table.

#### diagnostic_mapping

Loads diagnosis mapping reference data.

### Silver Layer

#### processed_patient_data

Joins patient records with diagnosis mapping data using diagnosis_code.

Features:

* Data enrichment
* Standardized healthcare dataset
* Ready for analytical processing

### Gold Layer

#### patient_statistics_by_diagnosis

Generates patient counts grouped by diagnosis.

#### patient_statistics_by_gender

Generates patient counts grouped by gender.

#### patient_statistics_by_admission_date

Generates patient counts grouped by admission date.

## Data Flow

```text
daily_patients
        +
diagnostic_mapping
        ↓
processed_patient_data
        ↓
patient_statistics_by_diagnosis

processed_patient_data
        ↓
patient_statistics_by_gender

processed_patient_data
        ↓
patient_statistics_by_admission_date
```

## DLT Features Implemented

* Declarative ETL Pipeline
* Automated Dependency Management
* Incremental Processing
* Materialized Views
* Data Lineage Tracking
* Pipeline Monitoring

## Pipeline Results

| Table Name                           | Layer  |
| ------------------------------------ | ------ |
| daily_patients                       | Bronze |
| diagnostic_mapping                   | Bronze |
| processed_patient_data               | Silver |
| patient_statistics_by_diagnosis      | Gold   |
| patient_statistics_by_gender         | Gold   |
| patient_statistics_by_admission_date | Gold   |

## Learning Outcomes

Through this project, I gained hands-on experience with:

* Databricks Delta Live Tables
* Medallion Architecture
* PySpark Transformations
* Data Modeling
* ETL Pipeline Development
* Healthcare Data Analytics

## Author

**Aishwarya K**

Aspiring Data Engineer | Databricks | Azure | PySpark | SQL

---