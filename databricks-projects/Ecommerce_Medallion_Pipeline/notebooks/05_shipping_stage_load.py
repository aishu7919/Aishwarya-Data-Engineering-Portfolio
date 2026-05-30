# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/incremental_load/shipping_data/source/"))

# COMMAND ----------

# Configuration

source_dir = "/Volumes/workspace/default/incremental_load/shipping_data/source/"

archive_dir = "/Volumes/workspace/default/incremental_load/shipping_data/archive/"

stage_table = "workspace.default.shipping_stage"

error_table = "workspace.default.shipping_errors"

print(f"Processing shipping data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

shipping_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("shipping_method", StringType(), True),
    StructField("shipping_cost", DecimalType(10,2), True),
    StructField("currency", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("package_weight", DecimalType(10,2), True),
    StructField("dimensions_cm", StringType(), True),
    StructField("estimated_delivery", DateType(), True),
    StructField("actual_delivery", DateType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

print("Shipping schema created successfully")

# COMMAND ----------

df_shipping = spark.read.schema(shipping_schema) \
    .csv(
        source_dir,
        header=True,
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )

df_shipping = df_shipping.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
).withColumn(
    "source_system",
    F.lit("ecommerce_shipping")
)

print(f"Total shipping records: {df_shipping.count()}")

display(df_shipping.limit(10))

# COMMAND ----------

# Validation

invalid_shipping_cost = df_shipping.filter(
    F.col("shipping_cost") < 0
).count()

print(f"Invalid shipping cost records: {invalid_shipping_cost}")

# Delivery status

df_shipping = df_shipping.withColumn(
    "delivery_status",
    F.when(
        F.col("actual_delivery").isNotNull(),
        "Delivered"
    ).otherwise("In Transit")
)

display(df_shipping.limit(10))

# COMMAND ----------

# Stage table

df_shipping.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(stage_table)

# Error table

df_invalid_shipping = df_shipping.filter(
    F.col("shipping_cost") < 0
)

df_invalid_shipping.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(error_table)

print("Shipping tables created")

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

