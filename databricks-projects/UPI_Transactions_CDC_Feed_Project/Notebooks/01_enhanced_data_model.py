# Databricks notebook source
# ============================================================
# CONFIGURATION SETUP
# ============================================================

catalog_name = "workspace"
schema_name = "default"

print(f"Using catalog: {catalog_name}, schema: {schema_name}")

# COMMAND ----------

# ============================================================
# RAW UPI TRANSACTIONS TABLE WITH CDC SUPPORT
# ============================================================

raw_table_sql = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.raw_upi_transactions_v1 (

    transaction_id STRING,
    upi_id STRING,
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,

    transaction_amount DOUBLE,
    transaction_currency STRING,
    transaction_timestamp TIMESTAMP,
    transaction_status STRING,
    payment_method STRING,

    device_type STRING,
    device_os STRING,
    app_version STRING,

    latitude DOUBLE,
    longitude DOUBLE,
    city STRING,
    state STRING,
    country STRING,

    customer_id STRING,
    age_group STRING,
    gender STRING,

    processing_fee DOUBLE,
    commission DOUBLE,

    created_at TIMESTAMP,
    updated_at TIMESTAMP
)

USING DELTA

TBLPROPERTIES (
    'delta.enableChangeDataFeed' = true
)
"""
spark.sql(raw_table_sql)
print("Raw UPI transactions table created")


# COMMAND ----------

# ============================================================
# MERCHANT AGGREGATIONS TABLE
# ============================================================

merchant_agg_sql = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.merchant_aggregations (

    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,

    aggregation_date DATE,

    total_transactions BIGINT,
    total_transaction_amount DOUBLE,

    successful_transactions BIGINT,
    failed_transactions BIGINT,

    total_processing_fee DOUBLE,
    total_commission DOUBLE,

    created_at TIMESTAMP,
    updated_at TIMESTAMP

)

USING DELTA
"""

spark.sql(merchant_agg_sql)

print("Merchant aggregations table created")

# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.default.merchant_hourly_aggregations
LIMIT 1
""").printSchema()

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").show(truncate=False)

# COMMAND ----------


spark.sql("SHOW TABLES IN default").show(truncate=False)

# COMMAND ----------

spark.table("default.merchant_aggregations").printSchema()

# COMMAND ----------

spark.sql("""
ALTER TABLE default.merchant_aggregations
ADD COLUMNS (
    aggregation_hour TIMESTAMP
)
""")

# COMMAND ----------

spark.table("default.merchant_aggregations").printSchema()

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY default.raw_upi_transactions_v1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _change_type,
# MAGIC     transaction_id,
# MAGIC     transaction_amount,
# MAGIC     _commit_version,
# MAGIC     _commit_timestamp
# MAGIC FROM table_changes(
# MAGIC     'default.raw_upi_transactions_v1',
# MAGIC     4
# MAGIC )
# MAGIC ORDER BY _commit_timestamp DESC

# COMMAND ----------

