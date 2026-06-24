# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import uuid

schema_name = "default"

raw_table = f"{schema_name}.raw_upi_transactions_v1"
merchant_agg_table = f"{schema_name}.merchant_aggregations"

print("Libraries imported and configuration set")
print(f"Target tables: {raw_table}, {merchant_agg_table}")

# COMMAND ----------

# =============================================================================
# DELTA TABLE MERGE FUNCTION
# =============================================================================

def merge_to_delta_table(delta_table_name: str, batch_df):
    """
    Idempotent merge function for Delta tables
    """
    
    delta_table = DeltaTable.forName(spark, delta_table_name)

    delta_table.alias("target").merge(
        batch_df.alias("source"),
        "target.merchant_id = source.merchant_id AND target.aggregation_date = source.aggregation_date AND target.aggregation_hour = source.aggregation_hour"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

print("Delta table merge function defined")

# COMMAND ----------

# Simple merchant aggregation function with CDC-aware logic
def process_merchant_aggregations(batch_df, batch_id):
    """Process merchant aggregations using CDC-aware approach"""
    try:
        print(f"Processing batch {batch_id} with {batch_df.count()} records")

        filtered_df = batch_df.filter(
            (F.col("transaction_id").isNotNull()) &
            (F.col("merchant_id").isNotNull()) &
            (F.col("transaction_amount").isNotNull()) &
            (F.col("transaction_amount") > 0) &
            (F.col("transaction_timestamp").isNotNull()) &
            (F.col("transaction_status").isNotNull()) &
            (F.col("_change_type").isin("insert","delete","update_postimage"))
        )

        if filtered_df.count() == 0:
            print("No valid records found in batch")
            return

        cdc_df = filtered_df.withColumn(
            "cdc_multiplier",
            F.when(F.col("_change_type") == "insert", 1)
             .when(F.col("_change_type") == "delete", -1)
             .otherwise(0)
        )

        merchant_aggregations = cdc_df.groupBy(
            F.col("merchant_id"),
            F.col("merchant_name"),
            F.col("merchant_category"),
            F.date_trunc("hour", F.col("transaction_timestamp")).alias("aggregation_hour"),
            F.to_date(F.col("transaction_timestamp")).alias("aggregation_date")
        ).agg(
            F.sum("cdc_multiplier").alias("total_transactions"),

            F.sum(
                F.when(F.col("transaction_status") == "completed",
                       F.col("cdc_multiplier")).otherwise(0)
            ).alias("successful_transactions"),

            F.sum(
                F.when(F.col("transaction_status") == "failed",
                       F.col("cdc_multiplier")).otherwise(0)
            ).alias("failed_transactions"),

            F.sum(
                F.when(F.col("transaction_status") == "refunded",
                       F.col("cdc_multiplier")).otherwise(0)
            ).alias("refunded_transactions"),

            F.sum(
                F.col("transaction_amount") * F.col("cdc_multiplier")
            ).alias("total_transaction_amount"),

            F.sum(
                F.when(F.col("transaction_status") == "completed",
                       F.col("transaction_amount") * F.col("cdc_multiplier"))
                .otherwise(0)
            ).alias("successful_transaction_amount"),

            F.sum(
                F.coalesce(F.col("processing_fee"), F.lit(0))
                * F.col("cdc_multiplier")
            ).alias("total_processing_fee"),

            F.sum(
                F.coalesce(F.col("commission"), F.lit(0))
                * F.col("cdc_multiplier")
            ).alias("total_commission")
        )

        merchant_aggregations = merchant_aggregations \
            .withColumn(
                "success_rate",
                F.when(
                    F.col("total_transactions") > 0,
                    (F.col("successful_transactions") /
                     F.col("total_transactions")) * 100
                ).otherwise(0)
            ) \
            .withColumn(
                "created_at",
                F.current_timestamp()
            ) \
            .withColumn(
                "updated_at",
                F.current_timestamp()
            )

        merge_to_delta_table(
            merchant_agg_table,
            merchant_aggregations
        )

        records_processed = merchant_aggregations.count()

        print(
            f"Merchant aggregations processed: {records_processed} records"
        )

    except Exception as e:
        print(
            f"Error processing merchant aggregations: {str(e)}"
        )
        raise

print("Merchant aggregation processing function defined")

# COMMAND ----------

# =============================================================================
# STREAMING PIPELINE
# =============================================================================

def start_streaming_pipeline():
    """Start CDC streaming pipeline"""

    try:
        print("Starting streaming pipeline...")

        streaming_df = (
            spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(raw_table)
        )

        def process_batch(batch_df, batch_id):

            print(f"Processing batch {batch_id}")

            process_merchant_aggregations(
                batch_df,
                batch_id
            )

            print(
                f"Batch {batch_id} processed successfully"
            )

        query = (
            streaming_df.writeStream
            .foreachBatch(process_batch)
            .trigger(availableNow=True)
            .option(
                "checkpointLocation",
                "/Volumes/workspace/default/streaming_checkpoints/merchant_agg"
)           
            .start()
        )

        print("Streaming pipeline started successfully")

        return query

    except Exception as e:

        print(
            f"Error starting streaming pipeline: {str(e)}"
        )

        raise

print("Streaming pipeline function defined")

# COMMAND ----------

spark.sql("""
select count(*) as total_records
from default.raw_upi_transactions_v1
""").show()


# COMMAND ----------

query = start_streaming_pipeline()

# COMMAND ----------

query.exception()

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS default.streaming_checkpoints
""")


# COMMAND ----------

spark.sql("SHOW VOLUMES").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

spark.sql("SELECT current_catalog()").show()

# COMMAND ----------

spark.sql("SELECT current_schema()").show()

# COMMAND ----------

spark.sql("""
SELECT *
FROM default.merchant_aggregations
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT
merchant_name,
total_transactions,
total_transaction_amount,
successful_transactions
FROM default.merchant_aggregations
ORDER BY total_transaction_amount DESC
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*)
FROM default.merchant_aggregations
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM default.merchant_aggregations
# MAGIC ORDER BY updated_at DESC

# COMMAND ----------

