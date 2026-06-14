import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="daily_patients"
)
def daily_patients():
    return spark.read.table(
        "workspace.healthcare_bronze.bronze_patients"
    )
@dlt.table(
    name="diagnostic_mapping"
)
def diagnostic_mapping():
    return spark.read.table(
        "workspace.healthcare_bronze.bronze_diagnosis_mapping"
    )

@dlt.table(
    name="processed_patient_data"
)
def processed_patient_data():

    return (
        dlt.read("daily_patients")
        .join(
            dlt.read("diagnostic_mapping"),
            on="diagnosis_code",
            how="left"
        )
    )

@dlt.table(
    name="patient_statistics_by_diagnosis"
)
def patient_statistics_by_diagnosis():

    return (
        dlt.read("processed_patient_data")
        .groupBy("diagnosis_description")
        .count()
    )
@dlt.table(
    name="patient_statistics_by_gender"
)
def patient_statistics_by_gender():

    return (
        dlt.read("processed_patient_data")
        .groupBy("gender")
        .count()
    )
@dlt.table(
    name="patient_statistics_by_admission_date"
)
def patient_statistics_by_admission_date():

    return (
        dlt.read("processed_patient_data")
        .groupBy("admission_date")
        .count()
    )

