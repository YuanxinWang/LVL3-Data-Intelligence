import sys
import os
from pyspark.sql.functions import col, explode_outer, current_timestamp

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils


# subset allows na in non-critical columns
def process_flights(spark):
    print("Loading FLIGHT STATUS data into Silver...")
    sys.stdout.flush()

    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_flight_status")
    silver_df = df.select(
        explode_outer("FlightStatusResource.Flights.Flight").alias("fl")
    ).select(
        col("fl.OperatingCarrier.AirlineID").alias("airline_id"),
        col("fl.OperatingCarrier.FlightNumber").alias("flight_number"),
        col("fl.Departure.ScheduledTimeUTC.DateTime").cast("timestamp").alias("scheduled_departure_utc"),

        col("fl.Departure.AirportCode").alias("dep_airport_code"),
        col("fl.Departure.ActualTimeUTC.DateTime").cast("timestamp").alias("actual_departure_utc"),
        col("fl.Departure.Terminal.Name").alias("dep_terminal"),
        col("fl.Departure.Terminal.Gate").alias("dep_gate"),
        col("fl.Departure.TimeStatus.Code").alias("dep_time_status"),

        col("fl.Arrival.AirportCode").alias("arr_airport_code"),
        col("fl.Arrival.ScheduledTimeUTC.DateTime").cast("timestamp").alias("scheduled_arrival_utc"),
        col("fl.Arrival.ActualTimeUTC.DateTime").cast("timestamp").alias("actual_arrival_utc"),
        col("fl.Arrival.Terminal.Name").alias("arr_terminal"),
        col("fl.Arrival.Terminal.Gate").alias("arr_gate"),
        col("fl.Arrival.TimeStatus.Code").alias("arr_time_status"),

        col("fl.Equipment.AircraftCode").alias("aircraft_code"),
        col("fl.Equipment.AircraftRegistration").alias("aircraft_registration"),
        col("fl.FlightStatus.Code").alias("flight_status_code"),
        col("fl.FlightStatus.Definition").alias("flight_status_def")
    ).dropDuplicates(
        ["airline_id", "flight_number", "scheduled_departure_utc"]
    ).na.drop(
        subset=["airline_id", "flight_number", "scheduled_departure_utc"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_fact_flights"

    composite_pk = ["airline_id", "flight_number", "scheduled_departure_utc"]
    delta_utils.upsert_to_delta(spark, silver_df, target_table, composite_pk)

    print(f"Successfully loaded FLIGHT STATUS into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    process_flights(spark)
