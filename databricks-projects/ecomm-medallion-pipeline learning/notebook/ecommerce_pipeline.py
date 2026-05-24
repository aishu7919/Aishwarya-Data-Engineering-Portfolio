# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Medallion Pipeline

# COMMAND ----------

orders_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/ecommerce_raw/orders_2025_01_15.csv")

display(orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer

# COMMAND ----------

orders_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")

# COMMAND ----------

display(
    spark.read.table("bronze_orders").limit(10)
)

# COMMAND ----------

bronze_df = spark.read.table("bronze_orders")

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = bronze_df.filter(
    col("order_id").isNotNull()
)

# COMMAND ----------

print(bronze_df.columns)

# COMMAND ----------

from pyspark.sql.functions import col, upper, to_date

silver_df = bronze_df.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    to_date(col("order_date")).alias("order_date"),
    col("order_amount").cast("double"),
    upper(col("currency")).alias("currency"),
    upper(col("payment_method")).alias("payment_method"),
    col("shipping_address"),
    upper(col("order_status")).alias("order_status"),
    col("created_timestamp")
)

display(silver_df.limit(10))

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg

gold_df = silver_df.groupBy(
    "payment_method",
    "order_status"
).agg(
    count("order_id").alias("total_orders"),
    sum("order_amount").alias("total_revenue"),
    avg("order_amount").alias("avg_order_value")
)

display(gold_df)

# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_order_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Load

# COMMAND ----------

day2_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/ecommerce_raw/orders_2025_01_16.csv")

display(day2_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, upper, to_date

day2_silver_df = day2_df.select(
    col("order_id").cast("string"),
    col("customer_id").cast("string"),
    col("product_id").cast("string"),
    to_date(col("order_date")).alias("order_date"),
    col("order_amount").cast("double"),
    upper(col("currency")).alias("currency"),
    upper(col("payment_method")).alias("payment_method"),
    col("shipping_address"),
    upper(col("order_status")).alias("order_status"),
    col("created_timestamp")
)

display(day2_silver_df.limit(10))

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forName(
    spark,
    "silver_orders"
)

silver_delta.alias("target").merge(
    day2_silver_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_orders")

# COMMAND ----------

from pyspark.sql.functions import col, upper, to_date

day2_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/ecommerce_raw/orders_2025_01_16.csv")

day2_silver_df = day2_df.select(
    col("order_id").cast("string"),
    col("customer_id").cast("string"),
    col("product_id").cast("string"),
    to_date(col("order_date")).alias("order_date"),
    col("order_amount").cast("double"),
    upper(col("currency")).alias("currency"),
    upper(col("payment_method")).alias("payment_method"),
    col("shipping_address"),
    upper(col("order_status")).alias("order_status"),
    col("created_timestamp")
)

spark.sql("DROP TABLE IF EXISTS silver_orders")

day2_silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

display(
    spark.read.table("silver_orders")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Analytics

# COMMAND ----------

silver_df = spark.read.table("silver_orders")

from pyspark.sql.functions import sum, count, avg

gold_df = silver_df.groupBy(
    "payment_method",
    "order_status"
).agg(
    count("order_id").alias("total_orders"),
    sum("order_amount").alias("total_revenue"),
    avg("order_amount").alias("avg_order_value")
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_order_analytics")

display(gold_df)

# COMMAND ----------

