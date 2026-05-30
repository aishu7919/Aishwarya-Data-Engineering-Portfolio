import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def job_process(
    env,
    bq_project,
    bq_dataset,
    transformed_table,
    route_insights_table,
    origin_insights_table
):

    spark = None

    try:

        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("FlightBookingAnalysis") \
            .config("spark.sql.catalogImplementation", "hive") \
            .getOrCreate()

        logger.info("Spark session initialized.")

        # Input Path
        input_path = "gs://aishwarya-dataproc-exercise1-7919/flights_project/data/flight_booking.csv"

        logger.info(f"Input path resolved: {input_path}")

        # Read CSV
        data = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )

        logger.info("Data read successfully from GCS.")

        # Transformations
        transformed_data = data.withColumn(
            "is_weekend",
            when(
                col("flight_day").isin("Sat", "Sun"),
                lit(1)
            ).otherwise(lit(0))
        ).withColumn(
            "lead_time_category",
            when(
                col("purchase_lead") < 7,
                lit("Last-Minute")
            ).when(
                (col("purchase_lead") >= 7) &
                (col("purchase_lead") < 30),
                lit("Short-Term")
            ).otherwise(
                lit("Long-Term")
            )
        ).withColumn(
            "booking_success_rate",
            expr("booking_complete / num_passengers")
        )

        logger.info("Transformations completed.")

        # Route Insights
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

        # Booking Origin Insights
        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )

        logger.info("Aggregations completed.")

        # Write output to GCS
        output_path = "gs://aishwarya-dataproc-exercise1-7919/flights_project/output/transformed_data"

        transformed_data.write \
            .mode("overwrite") \
            .parquet(output_path)

        logger.info(f"Data written successfully to {output_path}")

    except Exception as e:

        logger.error(f"An error occurred: {e}")
        sys.exit(1)

    finally:

        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Flight Booking Analysis"
    )

    parser.add_argument("--env", required=True)
    parser.add_argument("--bq_project", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--transformed_table", required=True)
    parser.add_argument("--route_insights_table", required=True)
    parser.add_argument("--origin_insights_table", required=True)

    args = parser.parse_args()

    job_process(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table
    )