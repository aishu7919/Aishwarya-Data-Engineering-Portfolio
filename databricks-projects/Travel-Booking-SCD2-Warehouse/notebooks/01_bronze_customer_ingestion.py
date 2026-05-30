# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - BRONZE LAYER: CUSTOMER DATA INGESTION
# =============================================================================
# This notebook ingests customer master data into the bronze layer with SCD2 preparation
# Purpose: Raw customer data ingestion with SCD2 temporal columns for dimension processing
# Data Quality: Adds SCD2 metadata columns for historical tracking
# Output: Creates/updates bronze.customer_inc table with SCD2-ready customer data

# COMMAND ----------

import datetime as _dt

try:
    arrival_date = dbutils.widgets.get("arrival_date")
except Exception:
    arrival_date = _dt.date.today().strftime("%Y-%m-%d")

try:
    catalog = dbutils.widgets.get("catalog")
except Exception:
    catalog = "workspace"

try:
    schema = dbutils.widgets.get("schema")
except Exception:
    schema = "default"

try:
    base_volume = dbutils.widgets.get("base_volume")
except Exception:
    base_volume = f"/Volumes/{catalog}/{schema}/travel_raw"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# =============================================================================
# DATA INGESTION CONFIGURATION
# =============================================================================

customer_path = f"{base_volume}/customers_data/source/customers_2025-09-23.csv"

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("multiLine", "true")
    .load(customer_path)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# =============================================================================
# SCD2 TEMPORAL COLUMNS PREPARATION
# =============================================================================

out = (
    df
    .withColumn("valid_from", F.to_date(F.lit(arrival_date)))
    .withColumn("valid_to", F.to_date(F.lit("9999-12-31")))
    .withColumn("is_current", F.lit(True))
    .withColumn("business_date", F.to_date(F.lit(arrival_date)))
)

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze"
)

# COMMAND ----------

out.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.bronze.customer_inc")

# COMMAND ----------

print(f"Ingested rows: {out.count()}")

# COMMAND ----------

display(
    spark.table(f"{catalog}.bronze.customer_inc")
)

# COMMAND ----------

