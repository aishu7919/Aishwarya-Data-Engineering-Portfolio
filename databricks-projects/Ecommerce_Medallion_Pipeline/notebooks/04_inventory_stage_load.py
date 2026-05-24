# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/incremental_load/inventory_data/source/"))

# COMMAND ----------

# Configuration

source_dir = "/Volumes/workspace/default/incremental_load/inventory_data/source/"

archive_dir = "/Volumes/workspace/default/incremental_load/inventory_data/archive/"

stage_table = "workspace.default.inventory_stage"

error_table = "workspace.default.inventory_errors"

print(f"Processing inventory data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

inventory_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("stock_status", StringType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

print("Inventory schema created successfully")

# COMMAND ----------

df_inventory = spark.read.schema(inventory_schema) \
    .csv(
        source_dir,
        header=True,
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )

df_inventory = df_inventory.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
).withColumn(
    "source_system",
    F.lit("ecommerce_inventory")
)

print(f"Total inventory records: {df_inventory.count()}")

display(df_inventory.limit(10))

# COMMAND ----------

# Validation

invalid_stock = df_inventory.filter(
    F.col("stock_quantity") < 0
).count()

print(f"Invalid stock records: {invalid_stock}")

# Inventory category

df_inventory = df_inventory.withColumn(
    "inventory_category",
    F.when(
        F.col("stock_quantity") == 0,
        "Out Of Stock"
    ).when(
        F.col("stock_quantity") < 20,
        "Low Stock"
    ).otherwise("Healthy Stock")
)

display(df_inventory.limit(10))

# COMMAND ----------

# Stage table

df_inventory.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(stage_table)

# Error table

df_invalid_inventory = df_inventory.filter(
    F.col("stock_quantity") < 0
)

df_invalid_inventory.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(error_table)

print("Inventory tables created")

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

