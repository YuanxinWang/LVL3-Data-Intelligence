import sys
import os
from pyspark.sql.functions import col, explode_outer, current_timestamp, to_timestamp

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils


# subset allows na in non-critical columns

# flatten JSON
# OOM -> Decoupling. Seperate action flatten and casting
def _extract_flight_features(df_raw):
    df_extracted = df_raw.select(
        explode_outer("FlightStatusResource.Flights.Flight").alias("fl")
    ).select(
        col("fl.OperatingCarrier.AirlineID").cast("string").alias("airline_id"),
        col("fl.OperatingCarrier.FlightNumber").cast("string").alias("flight_number"),
        col("fl.Departure.ScheduledTimeUTC.DateTime").cast("string").alias("sched_dep_str"),
        
        col("fl.Departure.AirportCode").cast("string").alias("dep_airport_code"),
        col("fl.Departure.ActualTimeUTC.DateTime").cast("string").alias("act_dep_str"),
        col("fl.Departure.Terminal.Name").cast("string").alias("dep_terminal"),
        col("fl.Departure.Terminal.Gate").cast("string").alias("dep_gate"),
        col("fl.Departure.TimeStatus.Code").cast("string").alias("dep_time_status_code"),
        col("fl.Departure.TimeStatus.Definition").cast("string").alias("dep_time_status_desc"),
        
        col("fl.Arrival.AirportCode").cast("string").alias("arr_airport_code"),
        col("fl.Arrival.ScheduledTimeUTC.DateTime").cast("string").alias("sched_arr_str"),
        col("fl.Arrival.ActualTimeUTC.DateTime").cast("string").alias("act_arr_str"),
        col("fl.Arrival.Terminal.Name").cast("string").alias("arr_terminal"),
        col("fl.Arrival.Terminal.Gate").cast("string").alias("arr_gate"),
        col("fl.Arrival.TimeStatus.Code").cast("string").alias("arr_time_status_code"),
        col("fl.Arrival.TimeStatus.Definition").cast("string").alias("arr_time_status_desc"),
        
        col("fl.Equipment.AircraftCode").cast("string").alias("aircraft_code"),
        col("fl.Equipment.AircraftRegistration").cast("string").alias("aircraft_registration"),
        col("fl.FlightStatus.Code").cast("string").alias("flight_status_code"),
        col("fl.FlightStatus.Definition").cast("string").alias("flight_status_desc")
    )
    return df_extracted


# clean up duplication and key na
# cast timestamp & add new timestamp
# filter instead of na.drop -> save calculation
# Explicit Schema Declaration -> select instead of withColumn
# (everytime .withColumn, we are creating a new DataFrame. We have ~20 cols -> OOM. Select instead)
def _clean_and_cast_flights(df_extracted):
    df_filtered = df_extracted.filter(
        col("airline_id").isNotNull() &
        col("flight_number").isNotNull() &
        col("sched_dep_str").isNotNull()
    )

    timestemp_format = "yyyy-MM-dd'T'HH:mm'Z'"
    df_casted = df_filtered.select(
        col("airline_id"),
        col("flight_number"),
        to_timestamp(col("sched_dep_str"), timestemp_format).alias("scheduled_departure_utc"),

        col("dep_airport_code"),
        to_timestamp(col("act_dep_str"), timestemp_format).alias("actual_departure_utc"),
        col("dep_terminal"),
        col("dep_gate"),
        col("dep_time_status_code"),
        col("dep_time_status_desc"),

        col("arr_airport_code"),
        to_timestamp(col("sched_arr_str"), timestemp_format).alias("scheduled_arrival_utc"),
        to_timestamp(col("act_arr_str"), timestemp_format).alias("actual_arrival_utc"),
        col("arr_terminal"),
        col("arr_gate"),
        col("arr_time_status_code"),
        col("arr_time_status_desc"),

        col("aircraft_code"),
        col("aircraft_registration"),
        col("flight_status_code"),
        col("flight_status_desc")
    )

    silver_flights = df_casted.dropDuplicates(
        ["airline_id", "flight_number", "scheduled_departure_utc"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )
    return silver_flights


def process_flights(spark):
    print("Loading FLIGHT STATUS data into Silver...")
    sys.stdout.flush()

    df_raw = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_flight_status")
    df_extracted = _extract_flight_features(df_raw)
    silver_flights = _clean_and_cast_flights(df_extracted)

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_fact_flights"
    composite_pk = ["airline_id", "flight_number", "scheduled_departure_utc"]

    delta_utils.upsert_to_delta(spark, silver_flights, target_table, composite_pk)

    print(f"Successfully loaded FLIGHT STATUS into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    process_flights(spark)
