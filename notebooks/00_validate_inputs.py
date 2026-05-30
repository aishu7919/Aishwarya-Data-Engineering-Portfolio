# Databricks notebook source
booking_count = spark.table("workspace.bronze.booking_inc").count()
customer_count = spark.table("workspace.bronze.customer_inc").count()

print(f"Booking records: {booking_count}")
print(f"Customer records: {customer_count}")

assert booking_count > 0, "Bookings file is empty"
assert customer_count > 0, "Customers file is empty"

print("Input validation passed")

# COMMAND ----------

