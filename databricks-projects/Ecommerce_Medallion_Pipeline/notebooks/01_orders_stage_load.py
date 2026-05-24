# Databricks notebook source
# Configuration

source_dir = "/Volumes/workspace/default/incremental_load/orders_data/source/"

archive_dir = "/Volumes/workspace/default/incremental_load/orders_data/archive/"

stage_table = "workspace.default.orders_stage"

error_table = "workspace.default.orders_errors"

print(f"Processing orders data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

# Test path

display(dbutils.fs.ls(source_dir))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_amount", DecimalType(10,2), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

print("Orders schema created successfully")

# COMMAND ----------

# Read and validate orders data

df_orders = spark.read.schema(orders_schema) \
    .csv(
        source_dir,
        header=True,
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )

# Add processing metadata

df_orders = df_orders.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
).withColumn(
    "source_system",
    F.lit("ecommerce_orders")
)

# Data quality checks

total_records = df_orders.count()

null_order_ids = df_orders.filter(
    F.col("order_id").isNull()
).count()

null_customer_ids = df_orders.filter(
    F.col("customer_id").isNull()
).count()

invalid_amounts = df_orders.filter(
    F.col("order_amount") <= 0
).count()

print(f"Total records processed: {total_records}")
print(f"Null order IDs: {null_order_ids}")
print(f"Null customer IDs: {null_customer_ids}")
print(f"Invalid amounts: {invalid_amounts}")

display(df_orders.limit(10))

# COMMAND ----------

# Filter valid records

df_valid_orders = df_orders.filter(
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("order_amount") > 0)
)

# Filter invalid records

df_invalid_orders = df_orders.filter(
    (F.col("order_id").isNull()) |
    (F.col("customer_id").isNull()) |
    (F.col("order_amount") <= 0)
)

valid_records = df_valid_orders.count()
invalid_records = df_invalid_orders.count()

print(f"Valid records: {valid_records}")
print(f"Invalid records: {invalid_records}")

# COMMAND ----------

df_valid_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(stage_table)

print("Valid orders written to staging table")

# COMMAND ----------

df_invalid_orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(error_table)

print("Invalid orders written to error table")

# COMMAND ----------

display(spark.table(stage_table))

# COMMAND ----------

display(spark.table(error_table))

# COMMAND ----------

files = dbutils.fs.ls(source_dir)

for file in files:
    file_name = file.name
    
    dbutils.fs.mv(
        source_dir + file_name,
        archive_dir + file_name
    )
    
    print(f"Archived: {file_name}")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

