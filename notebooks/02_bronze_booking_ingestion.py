# Databricks notebook source
# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - BRONZE LAYER: BOOKING DATA INGESTION
# =============================================================================
# This notebook ingests booking transactional data into the bronze layer
# Purpose: Raw booking data ingestion for downstream fact table processing
# Data Quality: Incremental append ingestion with business date partitioning
# Output: Creates/updates bronze.booking_inc table

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

booking_path = f"{base_volume}/bookings_data/source/bookings_2025-09-23.csv"

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("multiLine", "true")
    .load(booking_path)
)

# COMMAND ----------

display(df)

# COMMAND ----------

# =============================================================================
# BUSINESS DATE PREPARATION
# =============================================================================

out = (
    df
    .withColumn("business_date", F.to_date(F.lit(arrival_date)))
)

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze"
)

# COMMAND ----------

out.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.bronze.booking_inc")

# COMMAND ----------

print(f"Ingested rows: {out.count()}")

# COMMAND ----------

display(
    spark.table(f"{catalog}.bronze.booking_inc")
)

# COMMAND ----------

