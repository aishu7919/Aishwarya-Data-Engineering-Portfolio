# Databricks notebook source
display(
    spark.table("workspace.silver.fact_bookings")
)

# COMMAND ----------

spark.sql("""
ANALYZE TABLE workspace.silver.fact_bookings
COMPUTE STATISTICS
""")

# COMMAND ----------

spark.sql("""
DESCRIBE EXTENDED workspace.silver.fact_bookings
""")

# COMMAND ----------

