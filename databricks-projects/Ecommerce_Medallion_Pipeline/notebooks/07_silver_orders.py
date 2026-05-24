# Databricks notebook source
from pyspark.sql import functions as F

orders_df = spark.table("workspace.default.orders_stage")

customers_df = spark.table("workspace.default.customers_stage")

products_df = spark.table("workspace.default.products_stage")

inventory_df = spark.table("workspace.default.inventory_stage")

shipping_df = spark.table("workspace.default.shipping_stage")

print("All stage tables loaded")

# COMMAND ----------

silver_orders_df = orders_df.alias("o") \
    .join(
        customers_df.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    ) \
    .join(
        products_df.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    ) \
    .join(
        inventory_df.alias("i"),
        F.col("o.product_id") == F.col("i.product_id"),
        "left"
    )

# COMMAND ----------

silver_orders_df = silver_orders_df.select(
    F.col("o.order_id"),
    F.col("o.order_date"),
    F.col("o.order_amount"),
    F.col("o.order_status"),

    F.col("c.customer_id"),
    F.col("c.first_name"),
    F.col("c.last_name"),
    F.col("c.customer_tier"),
    F.col("c.lifecycle_stage"),

    F.col("p.product_id"),
    F.col("p.product_name"),
    F.col("p.category"),
    F.col("p.brand"),
    F.col("p.price_category"),

    F.col("i.stock_quantity"),
    F.col("i.inventory_category")
)

display(silver_orders_df.limit(10))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.default.silver_orders")

# COMMAND ----------

silver_orders_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_orders")

print("Silver orders table created successfully")

# COMMAND ----------

display(
    spark.table("workspace.default.silver_orders")
)

# COMMAND ----------

