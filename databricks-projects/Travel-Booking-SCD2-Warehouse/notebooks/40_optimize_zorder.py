# Databricks notebook source
spark.sql("""
OPTIMIZE workspace.silver.fact_bookings
ZORDER BY (customer_id)
""")

# COMMAND ----------

