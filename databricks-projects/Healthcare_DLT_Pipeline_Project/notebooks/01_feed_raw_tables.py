# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

patients_df = spark.read \
    .option("header", "true") \
    .csv("/Volumes/workspace/healthcare_bronze/patient_files/patients_daily_file_1_2025.csv")

# COMMAND ----------

patients_df.display()

# COMMAND ----------

patients_1 = spark.read.option("header","true").csv(
    "/Volumes/workspace/healthcare_bronze/patient_files/patients_daily_file_1_2025.csv"
)

patients_2 = spark.read.option("header","true").csv(
    "/Volumes/workspace/healthcare_bronze/patient_files/patients_daily_file_2_2025.csv"
)

patients_3 = spark.read.option("header","true").csv(
    "/Volumes/workspace/healthcare_bronze/patient_files/patients_daily_file_3_2025.csv"
)

# COMMAND ----------

bronze_patients = (
    patients_1
    .union(patients_2)
    .union(patients_3)
)

# COMMAND ----------

bronze_patients.count()

# COMMAND ----------

bronze_patients.write \
    .mode("overwrite") \
    .saveAsTable("workspace.healthcare_bronze.bronze_patients")

# COMMAND ----------

diagnosis_df = spark.read.option("header","true").csv(
    "/Volumes/workspace/healthcare_bronze/patient_files/diagnosis_mapping.csv"
)

# COMMAND ----------

diagnosis_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.healthcare_bronze.bronze_diagnosis_mapping")

# COMMAND ----------

file1= patients_1.count()
file2= patients_2.count()
file3= patients_3.count()
print(file1,file2,file3)


# COMMAND ----------

