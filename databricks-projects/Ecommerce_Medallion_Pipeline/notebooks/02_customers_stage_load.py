# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/incremental_load/customers_data/source/"))

# COMMAND ----------

# Configuration

source_dir = "/Volumes/workspace/default/incremental_load/customers_data/source/"

archive_dir = "/Volumes/workspace/default/incremental_load/customers_data/archive/"

stage_table = "workspace.default.customers_stage"

error_table = "workspace.default.customers_errors"

print(f"Processing customers data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("registration_date", DateType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

print("Customer schema created successfully")

# COMMAND ----------

df_customers = spark.read.schema(customers_schema) \
    .csv(
        source_dir,
        header=True,
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )

# Add metadata columns

df_customers = df_customers.withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
).withColumn(
    "source_system",
    F.lit("ecommerce_customers")
)

print(f"Total customer records: {df_customers.count()}")

display(df_customers.limit(10))

# COMMAND ----------

# Email validation

invalid_emails = df_customers.filter(
    ~F.col("email").contains("@")
).count()

# Null customer IDs

null_customer_ids = df_customers.filter(
    F.col("customer_id").isNull()
).count()

print(f"Invalid emails: {invalid_emails}")
print(f"Null customer IDs: {null_customer_ids}")

# COMMAND ----------

# Calculate customer age

df_customers = df_customers.withColumn(
    "customer_age",
    F.floor(
        F.datediff(
            F.current_date(),
            F.col("date_of_birth")
        ) / 365
    )
)

# Customer lifecycle stage

df_customers = df_customers.withColumn(
    "lifecycle_stage",
    F.when(
        F.col("customer_tier") == "Premium",
        "High Value"
    ).when(
        F.col("customer_tier") == "Gold",
        "Loyal"
    ).otherwise("Standard")
)

display(df_customers.limit(10))

# COMMAND ----------

df_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(stage_table)

print("Customer stage table created successfully")

# COMMAND ----------

# Invalid customer records

df_invalid_customers = df_customers.filter(
    (F.col("customer_id").isNull()) |
    (~F.col("email").contains("@"))
)

df_invalid_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(error_table)

print("Customer error table created")

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

