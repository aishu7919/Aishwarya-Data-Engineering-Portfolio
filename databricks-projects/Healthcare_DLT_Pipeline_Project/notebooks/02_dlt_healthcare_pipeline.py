# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

patients_df = spark.table(
    "workspace.healthcare_bronze.bronze_patients"
)

diagnosis_df = spark.table(
    "workspace.healthcare_bronze.bronze_diagnosis_mapping"
)

# COMMAND ----------

patients_df.printSchema()

diagnosis_df.printSchema()

# COMMAND ----------

patients_df.display()

# COMMAND ----------

diagnosis_df.display()

# COMMAND ----------

silver_patients = (
    patients_df.join(
        diagnosis_df,
        on="diagnosis_code",
        how="left"
    )
)

# COMMAND ----------

silver_patients.display()

# COMMAND ----------

silver_patients.count()

# COMMAND ----------

silver_patients.write \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.healthcare_bronze.silver_patients"
    )

# COMMAND ----------

silver_patients.count()

# COMMAND ----------

silver_patients.write \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.healthcare_bronze.silver_patients"
    )

# COMMAND ----------

disease_summary = (
    silver_patients
    .groupBy("diagnosis_description")
    .count()
    .orderBy(col("count").desc())
)

# COMMAND ----------

disease_summary.display()

# COMMAND ----------

disease_summary.write \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.healthcare_bronze.gold_disease_summary"
    )

# COMMAND ----------

gender_disease_summary = (
    silver_patients
    .groupBy("gender", "diagnosis_description")
    .count()
    .orderBy("gender")
)

# COMMAND ----------

gender_disease_summary.display()

# COMMAND ----------

gender_disease_summary.write \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.healthcare_bronze.gold_gender_disease_summary"
    )

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

age_group_df = (
    silver_patients
    .withColumn(
        "age_group",
        when(col("age") < 30, "Under 30")
        .when((col("age") >= 30) & (col("age") <= 50), "30-50")
        .otherwise("Above 50")
    )
)

# COMMAND ----------

age_group_summary = (
    age_group_df
    .groupBy("age_group")
    .count()
)

# COMMAND ----------

age_group_summary.display()


# COMMAND ----------

age_group_summary.write \
    .mode("overwrite") \
    .saveAsTable(
        "workspace.healthcare_bronze.gold_age_group_summary"
    )

# COMMAND ----------

