# Databricks notebook source
from pyspark.sql.functions import col

# Read customer bronze table
customer_df = spark.table("workspace.bronze.customer_inc")

# Customer DQ checks
dq_results = {
    "null_customer_id":
        customer_df.filter(col("customer_id").isNull()).count(),

    "null_customer_name":
        customer_df.filter(col("customer_name").isNull()).count(),

    "invalid_email":
        customer_df.filter(~col("email").contains("@")).count(),

    "null_phone":
        customer_df.filter(col("phone_number").isNull()).count()
}

print(dq_results)

# COMMAND ----------

