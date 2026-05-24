# Databricks notebook source
trigger_path = "/Volumes/workspace/default/incremental_load/trigger_files/trigger_2025_01_15.json"

trigger_df = spark.read.option("multiline", "true").json(trigger_path)

display(trigger_df)

# COMMAND ----------

trigger_data = trigger_df.collect()[0]

print(trigger_data)

# COMMAND ----------

batch_ready = trigger_data["data_files_ready"]

print(f"Batch Ready Status: {batch_ready}")

# COMMAND ----------

if batch_ready == True:

    print("Pipeline execution started")

else:

    raise Exception("Data files not ready")

# COMMAND ----------

if batch_ready == True:

    print("Pipeline execution started")

else:

    raise Exception("Data files not ready")

# COMMAND ----------

dbutils.notebook.run(
    "01_orders_stage_load",
    0
)

dbutils.notebook.run(
    "02_customers_stage_load",
    0
)

dbutils.notebook.run(
    "03_products_stage_load",
    0
)

dbutils.notebook.run(
    "04_inventory_stage_load",
    0
)

dbutils.notebook.run(
    "05_shipping_stage_load",
    0
)

dbutils.notebook.run(
    "06_data_validation",
    0
)

dbutils.notebook.run(
    "07_silver_orders",
    0
)

dbutils.notebook.run(
    "08_gold_order_analytics",
    0
)

print("Full Ecommerce Pipeline Executed Successfully")

# COMMAND ----------

