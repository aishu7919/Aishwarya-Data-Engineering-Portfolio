# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/incremental_load/products_data/source/"))

# COMMAND ----------

# Configuration

source_dir = "/Volumes/workspace/default/incremental_load/products_data/source/"

archive_dir = "/Volumes/workspace/default/incremental_load/products_data/archive/"

stage_table = "workspace.default.products_stage"

error_table = "workspace.default.products_errors"

print(f"Processing products data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DecimalType(10,2), True),
    StructField("currency", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("weight_kg", DecimalType(10,2), True),
    StructField("dimensions_cm", StringType(), True),
    StructField("color", StringType(), True),
    StructField("material", StringType(), True),
    StructField("description", StringType(), True),
    StructField("launch_date", DateType(), True),
    StructField("discontinued", BooleanType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

print("Products schema created successfully")

# COMMAND ----------

df_products = spark.read.schema(products_schema) \
    .csv(
        source_dir,
        header=True,
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )

# Add metadata columns

df_products = df_products.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
).withColumn(
    "source_system",
    F.lit("ecommerce_products")
)

print(f"Total product records: {df_products.count()}")

display(df_products.limit(10))

# COMMAND ----------

# Validation checks

invalid_prices = df_products.filter(
    F.col("price") <= 0
).count()

null_product_ids = df_products.filter(
    F.col("product_id").isNull()
).count()

print(f"Invalid prices: {invalid_prices}")
print(f"Null product IDs: {null_product_ids}")

# COMMAND ----------

# Product lifecycle category

df_products = df_products.withColumn(
    "product_status",
    F.when(
        F.col("discontinued") == True,
        "Inactive"
    ).otherwise("Active")
)

# Price category

df_products = df_products.withColumn(
    "price_category",
    F.when(
        F.col("price") >= 1000,
        "Premium"
    ).when(
        F.col("price") >= 500,
        "Mid Range"
    ).otherwise("Budget")
)

display(df_products.limit(10))

# COMMAND ----------

df_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(stage_table)

print("Products stage table created")

# COMMAND ----------

df_invalid_products = df_products.filter(
    (F.col("product_id").isNull()) |
    (F.col("price") <= 0)
)

df_invalid_products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(error_table)

print("Products error table created")

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

