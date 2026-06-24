# Databricks notebook source
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import time

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS total_records
FROM default.raw_upi_transactions_v1
""").show()

# COMMAND ----------

# =============================================================================
# CONFIGURATION SETUP
# =============================================================================

schema_name = "default"

raw_table = f"{schema_name}.raw_upi_transactions_v1"

print(f"Target table: {raw_table}")

# COMMAND ----------

# =============================================================================
# MOCK DATA CONSTANTS
# =============================================================================

MERCHANTS = [
    {"merchant_id": "M001", "merchant_name": "Amazon India", "merchant_category": "E-commerce"},
    {"merchant_id": "M002", "merchant_name": "Swiggy", "merchant_category": "Food Delivery"},
    {"merchant_id": "M003", "merchant_name": "Uber", "merchant_category": "Transportation"},
    {"merchant_id": "M004", "merchant_name": "Netflix", "merchant_category": "Entertainment"},
    {"merchant_id": "M005", "merchant_name": "BigBasket", "merchant_category": "Grocery"},
    {"merchant_id": "M006", "merchant_name": "Flipkart", "merchant_category": "E-commerce"},
    {"merchant_id": "M007", "merchant_name": "Zomato", "merchant_category": "Food Delivery"},
    {"merchant_id": "M008", "merchant_name": "Ola", "merchant_category": "Transportation"}
]

UPI_IDS = [
    "user123@paytm",
    "user456@phonepe",
    "user789@googlepay",
    "user101@amazonpay",
    "user202@mobikwik"
]

CUSTOMER_IDS = [
    "CUST001",
    "CUST002",
    "CUST003",
    "CUST004",
    "CUST005"
]

PAYMENT_METHODS = ["UPI", "QR Code", "Mobile Number"]
DEVICE_TYPES = ["Mobile", "Tablet"]
OPERATING_SYSTEMS = ["Android", "iOS"]

CITIES = [
    "Mumbai",
    "Delhi",
    "Bangalore",
    "Chennai",
    "Kolkata",
    "Hyderabad",
    "Pune",
    "Ahmedabad"
]

STATES = [
    "Maharashtra",
    "Delhi",
    "Karnataka",
    "Tamil Nadu",
    "West Bengal",
    "Telangana",
    "Gujarat"
]

AGE_GROUPS = [
    "18-25",
    "26-35",
    "36-45",
    "46-55",
    "55+"
]

GENDERS = [
    "Male",
    "Female",
    "Other"
]

transaction_counter = 1

print("Mock data constants initialized")



# COMMAND ----------

def insert_new_transactions(num_transactions=5):

    global transaction_counter

    try:

        print(f"INSERT: Adding {num_transactions} new transactions...")

        transaction_data = []

        for i in range(num_transactions):

            merchant = random.choice(MERCHANTS)
            upi_id = random.choice(UPI_IDS)
            customer_id = random.choice(CUSTOMER_IDS)

            if merchant["merchant_category"] == "E-commerce":
                amount = round(random.uniform(100, 5000), 2)
            elif merchant["merchant_category"] == "Food Delivery":
                amount = round(random.uniform(50, 500), 2)
            elif merchant["merchant_category"] == "Transportation":
                amount = round(random.uniform(20, 200), 2)
            else:
                amount = round(random.uniform(50, 1000), 2)

            transaction_time = datetime.now() - timedelta(
                minutes=random.randint(1, 60)
            )

            status = random.choice(
                ["completed", "completed", "completed",
                 "failed", "refunded"]
            )

            processing_fee = round(amount * 0.005, 2)
            commission = round(amount * 0.01, 2)

            transaction_id = (
                f"TXN_{datetime.now().strftime('%Y%m%d')}_{transaction_counter:06d}"
            )

            transaction_counter += 1

            transaction_data.append(
                (
                    transaction_id,
                    upi_id,
                    merchant["merchant_id"],
                    merchant["merchant_name"],
                    merchant["merchant_category"],
                    float(amount),
                    "INR",
                    transaction_time,
                    status,
                    random.choice(PAYMENT_METHODS),
                    random.choice(DEVICE_TYPES),
                    random.choice(OPERATING_SYSTEMS),
                    customer_id,
                    float(processing_fee),
                    float(commission),
                    datetime.now(),
                    datetime.now()
                )
            )

        transaction_df = spark.createDataFrame(
            transaction_data,
            [
                "transaction_id",
                "upi_id",
                "merchant_id",
                "merchant_name",
                "merchant_category",
                "transaction_amount",
                "transaction_currency",
                "transaction_timestamp",
                "transaction_status",
                "payment_method",
                "device_type",
                "device_os",
                "customer_id",
                "processing_fee",
                "commission",
                "created_at",
                "updated_at"
            ]
        )

        transaction_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(raw_table)

        print(
            f"INSERT: Successfully added {num_transactions} transactions"
        )

        return True

    except Exception as e:

        print(f"INSERT failed: {str(e)}")

        return False


print("Insert function defined")

# COMMAND ----------

success = insert_new_transactions(5)

print("Insert Status:", success)

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS total_records
FROM default.raw_upi_transactions_v1
""").show()

# COMMAND ----------

insert_new_transactions(3)

# COMMAND ----------

globals().keys()

# COMMAND ----------

def update_transaction(txn_id,new_amount):
    spark.sql(f"""
    UPDATE default.raw_upi_transactions_v1
    SET transaction_amount = {new_amount}
    WHERE transaction_id = '{txn_id}'
    """)

# COMMAND ----------

def delete_transaction(txn_id):
    spark.sql(f"""
    DELETE FROM default.raw_upi_transactions_v1
    WHERE transaction_id = '{txn_id}'
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT transaction_id,
# MAGIC        transaction_amount
# MAGIC FROM default.raw_upi_transactions_v1
# MAGIC LIMIT 5

# COMMAND ----------

update_transaction(
    "copied_transaction_id",
    9999
)

# COMMAND ----------

delete_transaction(
    "copied_transaction_id"
)

# COMMAND ----------

