import sys
import os
import logging
import databricks.pipelines as dp
from pyspark.sql.functions import col, explode_outer, current_timestamp, to_timestamp

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------------
# 1. Base Extraction Stream (No Data Dropped Yet)
# -------------------------------------------------------------------------
@dp.view(name="v_flights_extracted_stream")
def v_flights_extracted_stream():
    logger.info("Extracting raw flights JSON into flattened structure...")
    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.flight_status"

    df_raw = spark.readStream.table(source_table)
    timestamp_format = "yyyy-MM-dd'T'HH:mm'Z'"

    df_extracted = (df_raw
        .withColumn("fl", explode_outer("FlightStatusResource.Flights.Flight"))

        .withColumn("airline_id", col("fl.OperatingCarrier.AirlineID").cast("string"))
        .withColumn("flight_number", col("fl.OperatingCarrier.FlightNumber").cast("string"))
        .withColumn("sched_dep_str", col("fl.Departure.ScheduledTimeUTC.DateTime").cast("string"))
        .withColumn("scheduled_departure_utc", to_timestamp(col("sched_dep_str"), timestamp_format))
        
        .withColumn("dep_airport_code", col("fl.Departure.AirportCode").cast("string"))
        .withColumn("act_dep_str", col("fl.Departure.ActualTimeUTC.DateTime").cast("string"))
        .withColumn("actual_departure_utc", to_timestamp(col("act_dep_str"), timestamp_format))
        .withColumn("dep_terminal", col("fl.Departure.Terminal.Name").cast("string"))
        .withColumn("dep_gate", col("fl.Departure.Terminal.Gate").cast("string"))
        .withColumn("dep_time_status_code", col("fl.Departure.TimeStatus.Code").cast("string"))
        .withColumn("dep_time_status_desc", col("fl.Departure.TimeStatus.Definition").cast("string"))
        
        .withColumn("arr_airport_code", col("fl.Arrival.AirportCode").cast("string"))
        .withColumn("sched_arr_str", col("fl.Arrival.ScheduledTimeUTC.DateTime").cast("string"))
        .withColumn("scheduled_arrival_utc", to_timestamp(col("sched_arr_str"), timestamp_format))
        .withColumn("act_arr_str", col("fl.Arrival.ActualTimeUTC.DateTime").cast("string"))
        .withColumn("actual_arrival_utc", to_timestamp(col("act_arr_str"), timestamp_format))
        .withColumn("arr_terminal", col("fl.Arrival.Terminal.Name").cast("string"))
        .withColumn("arr_gate", col("fl.Arrival.Terminal.Gate").cast("string"))
        .withColumn("arr_time_status_code", col("fl.Arrival.TimeStatus.Code").cast("string"))
        .withColumn("arr_time_status_desc", col("fl.Arrival.TimeStatus.Definition").cast("string"))
        
        .withColumn("aircraft_code", col("fl.Equipment.AircraftCode").cast("string"))
        .withColumn("aircraft_registration", col("fl.Equipment.AircraftRegistration").cast("string"))
        .withColumn("flight_status_code", col("fl.FlightStatus.Code").cast("string"))
        .withColumn("flight_status_desc", col("fl.FlightStatus.Definition").cast("string"))

        .select(
            "airline_id", "flight_number", "scheduled_departure_utc",
            "dep_airport_code", "actual_departure_utc", "dep_terminal", "dep_gate",
            "dep_time_status_code", "dep_time_status_desc",
            "arr_airport_code", "scheduled_arrival_utc", "actual_arrival_utc",
            "arr_terminal", "arr_gate",
            "arr_time_status_code", "arr_time_status_desc",
            "aircraft_code", "aircraft_registration",
            "flight_status_code", "flight_status_desc"
        )

        .dropDuplicates(["airline_id", "flight_number", "scheduled_departure_utc"])
    )
    return df_extracted


# -------------------------------------------------------------------------
# 2. Quarantine Stream (Dead Letter Queue for Bad Data)
# -------------------------------------------------------------------------
@dp.table(name="quarantine_flights", comment="Dead Letter Queue: Flights missing composite PKs")
def quarantine_flights():
    logger.warning("Routing invalid flights to quarantine table...")

    df = dp.readStream("v_flights_extracted_stream")
    return (df
        .filter(
            col("airline_id").isNull() | 
            col("flight_number").isNull() | 
            col("scheduled_departure_utc").isNull()
        )
        .withColumn("quarantined_at", current_timestamp())
    )


# -------------------------------------------------------------------------
# 3. Golden Path & AutoCDC Setup (Good Data)
# -------------------------------------------------------------------------
@dp.view(name="v_fact_flights_cdc_source")
def v_fact_flights_cdc_source():
    logger.info("Filtering valid flights for Silver fact table...")

    df = dp.readStream("v_flights_extracted_stream")
    return (df
        .filter(
            col("airline_id").isNotNull() & 
            col("flight_number").isNotNull() & 
            col("scheduled_departure_utc").isNotNull()
        )
        .withColumn("silver_processed_at", current_timestamp())
    )

dp.create_streaming_table(
    name="fact_flights",
    comment="Silver Fact Table for Flight Operations (AutoCDC managed)"
)

logger.info("Applying AutoCDC (SCD Type 1) to fact_flights using composite keys...")
dp.apply_changes(
    target="fact_flights",
    source="v_fact_flights_cdc_source",
    keys=["airline_id", "flight_number", "scheduled_departure_utc"],
    sequence_by=col("silver_processed_at"),
    apply_as_deletes=None,
    except_column_list=[]
)
