# 
# In this file, we do not use DRY stratagy to keep the clear structure, readability and maintanability.
# 

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


# sys.stdout.flush() force print
# explode reference data, drop duplication and na value. form into dim / tmp table
def process_airports(spark):
    print("Loading AIRPORTS reference data into Silver...")
    sys.stdout.flush()
    
    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_airports")
    silver_df = df.select(
        explode_outer("AirportResource.Airports.Airport").alias("ap")
    ).select(
        col("ap.AirportCode").alias("airport_code"),
        col("ap.Names.Name").getField("$").alias("airport_name"),
        col("ap.CityCode").alias("city_code"),
        col("ap.CountryCode").alias("country_code"),
        col("ap.Position.Coordinate.Latitude").cast("double").alias("latitude"),
        col("ap.Position.Coordinate.Longitude").cast("double").alias("longitude"),
        col("ap.UtcOffset").alias("time_zone_offset")
    ).dropDuplicates(
        ["airport_code"]
    ).na.drop(
        subset=["airport_code"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_airports"
    delta_utils.upsert_to_delta(spark, silver_df, target_table, "airport_code")

    print(f"Successfully loaded AIRPORTS into {target_table}")
    sys.stdout.flush()


def process_cities(spark):
    print("Loading CITIES reference data into Silver...")
    sys.stdout.flush()
    
    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_cities")
    silver_df = df.select(
        explode_outer("CityResource.Cities.City").alias("ct")
    ).select(
        col("ct.CityCode").alias("city_code"),
        col("ct.Names.Name").getField("$").alias("city_name")
    ).dropDuplicates(
        ["city_code"]
    ).na.drop(
        subset=["city_code"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_cities"
    delta_utils.upsert_to_delta(spark, silver_df, target_table, "city_code")

    print(f"Successfully loaded CITIES into {target_table}")
    sys.stdout.flush()


def process_countries(spark):
    print("Loading COUNTRIES reference data into Silver...")
    sys.stdout.flush()
    
    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_countries")
    silver_df = df.select(
        explode_outer("CountryResource.Countries.Country").alias("co")
    ).select(
        col("co.CountryCode").alias("country_code"),
        col("co.Names.Name").getField("$").alias("country_name")
    ).dropDuplicates(
        ["country_code"]
    ).na.drop(
        subset=["country_code"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_countries"
    delta_utils.upsert_to_delta(spark, silver_df, target_table, "country_code")

    print(f"Successfully loaded COUNTRIES into {target_table}")
    sys.stdout.flush()


def process_airlines(spark):
    print("Loading AIRLINES reference data into Silver...")
    sys.stdout.flush()
    
    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_airlines")
    silver_df = df.select(
        explode_outer("AirlineResource.Airlines.Airline").alias("al")
    ).select(
        col("al.AirlineID").alias("airline_code"),
        col("al.Names.Name").getField("$").alias("airline_name")
    ).dropDuplicates(
        ["airline_code"]
    ).na.drop(
        subset=["airline_code"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_dim_airlines"
    delta_utils.upsert_to_delta(spark, silver_df, target_table, "airline_code")

    print(f"Successfully loaded AIRLINES into {target_table}")
    sys.stdout.flush()


def process_aircraft(spark):
    print("Loading AIRCRAFT reference data into Silver...")
    sys.stdout.flush()
    
    df = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_aircraft")
    silver_df = df.select(
        explode_outer("AircraftResource.AircraftSummaries.AircraftSummary").alias("ac")
    ).select(
        col("ac.AircraftCode").alias("aircraft_code"),
        col("ac.Names.Name").getField("$").alias("aircraft_name")
    ).dropDuplicates(
        ["aircraft_code"]
    ).na.drop(
        subset=["aircraft_code"]
    ).withColumn(
        "silver_processed_at", current_timestamp()
    )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_dim_aircraft"
    delta_utils.upsert_to_delta(spark, silver_df, target_table, "aircraft_code")

    print(f"Successfully loaded AIRCRAFT into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    print("Starting Unified Silver Load for Reference Data...")
    sys.stdout.flush()
    
    process_airlines(spark)
    process_aircraft(spark)
    process_countries(spark)
    process_cities(spark)
    process_airports(spark)

    print("All reference data loaded into Silver successfully!")
    sys.stdout.flush()
