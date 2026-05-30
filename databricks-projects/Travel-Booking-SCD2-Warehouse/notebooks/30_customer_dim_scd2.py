# Databricks notebook source
from pyspark.sql.functions import *

# Read customer bronze table
customer_df = spark.table("workspace.bronze.customer_inc")

# Add SCD2 columns
scd2_df = (
    customer_df
    .withColumn("valid_from", current_date())
    .withColumn("valid_to", lit("9999-12-31"))
    .withColumn("is_current", lit(True))
)

display(scd2_df)

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS workspace.silver.dim_customers
""")

# COMMAND ----------

scd2_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.dim_customers")

# COMMAND ----------

display(
    spark.table("workspace.silver.dim_customers")
)

# COMMAND ----------

