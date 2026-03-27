import sys
import os
import logging
import databricks.pipelines as dp
from pyspark.sql.functions import col, explode_outer, current_timestamp

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -------------------------------------------------------------------------
# ARCHITECTURE NOTE: WHY @dp.view?
# -------------------------------------------------------------------------
# We replaced physical tmp tables with @dp.view. These staging views only 
# exist in memory during pipeline execution. They consume ZERO storage and 
# are automatically parsed by the Databricks Catalyst optimizer for the 
# final JOIN operation.
# -------------------------------------------------------------------------
@dp.view(name="v_cities_lookup")
def v_cities_lookup():
    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.reference_cities"
    df_raw = spark.read.table(source_table)
    view_cities = (df_raw
                   .withColumn("ct", explode_outer("CityResource.Cities.City"))
                   .withColumn("city_code", col("ct.CityCode"))
                   .withColumn("city_name", col("ct.Names.Name").getField("$"))
                   .select("city_code", "city_name")
                   .dropDuplicates(["city_code"])
                   .filter(col("city_code").isNotNull())
                   )
    return view_cities


@dp.view(name="v_countries_lookup")
def v_countries_lookup():
    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.reference_countries"
    df_raw = spark.read.table(source_table)
    view_countries = (df_raw
                      .withColumn("co", explode_outer("CountryResource.Countries.Country"))
                      .withColumn("country_code", col("co.CountryCode"))
                      .withColumn("country_name", col("co.Names.Name").getField("$"))
                      .select("country_code", "country_name")
                      .dropDuplicates(["country_code"])
                      .filter(col("country_code").isNotNull())
                      )
    return view_countries


# CDC Straming Source: to activate AutoCDC downstream, must use readStream here
@dp.view(name="v_airports_cdc_source")
def v_airports_cdc_source():
    logger.info("Extracting AIRPORTS and joining with lookup views...")

    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.reference_airports"
    df_raw = spark.readStream.table(source_table)
    df_ap = (df_raw
             .withColumn("ap", explode_outer("AirportResource.Airports.Airport"))
             .withColumn("airport_code", col("ap.AirportCode"))
             .withColumn("airport_name", col("ap.Names.Name").getField("$"))
             .withColumn("city_code", col("ap.CityCode"))
             .withColumn("country_code", col("ap.CountryCode"))
             .withColumn("latitude", col("ap.Position.Coordinate.Latitude").cast("double"))
             .withColumn("longitude", col("ap.Position.Coordinate.Longitude").cast("double"))
             .withColumn("time_zone_offset", col("ap.UtcOffset"))
             .select(
                 "airport_code", "airport_name", "city_code", "country_code",
                 "latitude", "longitude", "time_zone_offset"
                 )
             .dropDuplicates(["airport_code"])
             .filter(col("airport_code").isNotNull())
             .withColumn("silver_processed_at", current_timestamp())
             )

    df_ct = dp.read("v_cities_lookup")
    df_co = dp.read("v_countries_lookup")

    view_airports = (df_ap
                     .join(df_ct, on="city_code", how="left")
                     .join(df_co, on="country_code", how="left")
                     )
    return view_airports

dp.create_streaming_table(
    name="dim_airports",
    comment="Final Denormalized Airports Dimension (AutoCDC)"
)

# except_column_list=["test_column"] to skip columns that do not need update
logger.info("Applying AutoCDC (SCD Type 1) to dim_airports...")
dp.apply_changes(
    target="dim_airports",
    source="v_airports_cdc_source",
    keys=["airport_code"],
    sequence_by=col("silver_processed_at"),
    apply_as_deletes=None,
    except_column_list=[]
)


@dp.view(name="v_airlines_cdc_source")
def v_airlines_cdc_source():
    logger.info("Extracting AIRLINES views...")

    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.reference_airlines"
    df_raw = spark.readStream.table(source_table)
    view_airlines = (df_raw
                     .withColumn("al", explode_outer("AirlineResource.Airlines.Airline"))
                     .withColumn("airline_code", col("al.AirlineID"))
                     .withColumn("airline_name", col("al.Names.Name").getField("$"))
                     .select("airline_code", "airline_name")
                     .dropDuplicates(["airline_code"])
                     .filter(col("airline_code").isNotNull())
                     .withColumn("silver_processed_at", current_timestamp())
                     )
    return view_airlines

dp.create_streaming_table(
    name="dim_airlines",
    comment="Airlines Dimensions (AutoCDC)"
)

logger.info("Applying AutoCDC (SCD Type 1) to dim_airlines...")
dp.apply_changes(
    target="dim_airlines",
    source="v_airlines_cdc_source",
    keys=["airline_code"],
    sequence_by=col("silver_processed_at"),
    apply_as_deletes=None,
    except_column_list=[]
)


@dp.view(name="v_aircraft_cdc_source")
def v_aircraft_cdc_source():
    logger.info("Extracting AIRCRAFT views...")

    source_table = f"{config.CATALOG_NAME}.{config.SCHEMA_BRONZE}.reference_aircraft"
    df_raw = spark.readStream.table(source_table)
    view_aircraft = (df_raw
                     .withColumn("ac", explode_outer("AircraftResource.AircraftSummaries.AircraftSummary"))
                     .withColumn("aircraft_code", col("ac.AircraftCode"))
                     .withColumn("aircraft_name", col("ac.Names.Name").getField("$"))
                     .select("aircraft_code", "aircraft_name")
                     .dropDuplicates(["aircraft_code"])
                     .filter(col("aircraft_code").isNotNull())
                     .withColumn("silver_processed_at", current_timestamp())
                     )
    return view_aircraft

dp.create_streaming_table(
    name="dim_aircraft",
    comment="Aircraft Dimensions (AutoCDC)"
)

logger.info("Applying AutoCDC (SCD Type 1) to dim_aircraft...")
dp.apply_changes(
    target="dim_aircraft",
    source="v_aircraft_cdc_source",
    keys=["aircraft_code"],
    sequence_by=col("silver_processed_at"),
    apply_as_deletes=None,
    except_column_list=[]
)
