# Databricks notebook source
# MAGIC %pip install pydeequ

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import *

print("PyDeequ Working")

# COMMAND ----------

# =============================================================================
# TRAVEL BOOKING SCD2 MERGE PROJECT - DATA QUALITY CHECKS FOR BOOKINGS
# =============================================================================
# Purpose: Validate booking data quality using PyDeequ
# Checks:
# - Completeness
# - Non-negative amount
# - Valid booking status
# - Uniqueness

# COMMAND ----------

import os

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import *
from pydeequ.verification import *

# COMMAND ----------

df = spark.table("workspace.bronze.booking_inc")

display(df)

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

catalog = "workspace"

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table(f"{catalog}.bronze.booking_inc")

dq_results = {
    "null_booking_id": df.filter(col("booking_id").isNull()).count(),
    "negative_amount": df.filter(col("amount") < 0).count(),
    "null_customer_id": df.filter(col("customer_id").isNull()).count()
}

print(dq_results)

# COMMAND ----------

