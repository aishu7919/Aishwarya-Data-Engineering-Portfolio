# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

orders_df = spark.table("workspace.default.orders_stage")

customers_df = spark.table("workspace.default.customers_stage")

products_df = spark.table("workspace.default.products_stage")

inventory_df = spark.table("workspace.default.inventory_stage")

shipping_df = spark.table("workspace.default.shipping_stage")

print("All stage tables loaded successfully")

# COMMAND ----------

orphan_customers = orders_df.join(
    customers_df,
    on="customer_id",
    how="left_anti"
)

print(f"Orders with missing customers: {orphan_customers.count()}")

# COMMAND ----------

orphan_products = orders_df.join(
    products_df,
    on="product_id",
    how="left_anti"
)

print(f"Orders with missing products: {orphan_products.count()}")

# COMMAND ----------

out_of_stock_products = inventory_df.filter(
    F.col("stock_quantity") <= 0
)

print(f"Out of stock products: {out_of_stock_products.count()}")

# COMMAND ----------

missing_shipping = orders_df.join(
    shipping_df,
    on="order_id",
    how="left_anti"
)

print(f"Orders missing shipping records: {missing_shipping.count()}")

# COMMAND ----------

validation_summary = [
    ("Missing Customers", orphan_customers.count()),
    ("Missing Products", orphan_products.count()),
    ("Out Of Stock Products", out_of_stock_products.count()),
    ("Missing Shipping Records", missing_shipping.count())
]

validation_df = spark.createDataFrame(
    validation_summary,
    ["validation_type", "issue_count"]
)

display(validation_df)

# COMMAND ----------

display(
    orders_df.select("order_id").limit(5)
)

# COMMAND ----------

display(
    shipping_df.select("order_id").limit(5)
)

# COMMAND ----------

# Observation:
# Shipping dataset contains shipping IDs instead of matching order IDs.
# Hence all order-shipping joins fail during validation.

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

