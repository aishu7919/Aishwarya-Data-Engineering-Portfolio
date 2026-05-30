# Databricks notebook source
from pyspark.sql import functions as F

silver_orders_df = spark.table(
    "workspace.default.silver_orders"
)

print("Silver orders loaded successfully")

# COMMAND ----------

gold_orders_df = silver_orders_df.groupBy(
    "category",
    "customer_tier",
    "inventory_category"
).agg(
    F.countDistinct("order_id").alias("total_orders"),

    F.sum("order_amount").alias("total_revenue"),

    F.avg("order_amount").alias("avg_order_value"),

    F.countDistinct("customer_id").alias("unique_customers")
)

# COMMAND ----------

spark.sql(
    "DROP TABLE IF EXISTS workspace.default.gold_order_analytics"
)

# COMMAND ----------

gold_orders_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_order_analytics")

print("Gold analytics table created")

# COMMAND ----------

