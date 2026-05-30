# Databricks notebook source
from pyspark.sql.functions import *

booking_df = spark.table("workspace.bronze.booking_inc")

display(booking_df)

# COMMAND ----------

customer_dim = spark.table("workspace.silver.dim_customers")

display(customer_dim)

# COMMAND ----------

customer_dim = customer_dim.select(
    "customer_id",
    "customer_name",
    "customer_address",
    "phone_number",
    "email",
    "valid_from",
    "valid_to",
    "is_current"
)

# COMMAND ----------

fact_df = booking_df.join(
    customer_dim,
    on="customer_id",
    how="left"
)

display(fact_df)

# COMMAND ----------

print(fact_df.columns)

# COMMAND ----------

fact_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.fact_bookings")

# COMMAND ----------

display(
    spark.table("workspace.silver.fact_bookings")
)


# COMMAND ----------

