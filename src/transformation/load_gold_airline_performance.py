import sys
import os
from pyspark.sql import functions as F

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils
from src.shared.gold_core import add_business_features


# join: combine flights fact table with airline dimension table
# left join: keep all flights, match airline name if available, otherweise unknown
# care: in airline chart "airline_code", in flight chart "airline_id"
def _join_airline(df_new, df_dim_airline):
    df_airline_name = df_dim_airline.select(
        F.col("airline_code").alias("airline_id"),
        F.col("airline_name").alias("airline_name")
    )

    df_join = df_new.join(
        df_airline_name,
        on="airline_id",
        how="left"
    )

    df_final = df_join.fillna({"airline_name": "Unknown"})
    return df_final


# joined name, but still keep ID. Always use ID to design logic to avoid error
# round(..., 2) -> round to 2 decimal places 
def _group_by_date_and_airline(df_joined):
    df_group = df_joined.groupBy(
        "scheduled_date",
        "airline_id",
        "airline_name"
    ).agg(
        F.count("flight_number").alias("total_flights"),
        F.sum(F.when(F.col("status_category") == "On_time", 1).otherwise(0)).alias("on_time_flights"),
        F.sum(F.when(F.col("status_category") == "Cancelled", 1).otherwise(0)).alias("cancelled_flights"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
        F.sum(F.when(F.col("is_morning_wave") == True, F.col("delay_minutes")).otherwise(0)).alias("morning_delay_minutes")
    )

    df_on_time = df_group.withColumn(
        "on_time_percentage",
        F.round((F.col("on_time_flights") / F.col("total_flights")) * 100, 2)
    )

    df_final = df_on_time.withColumn(
        "gold_processed_at", F.current_timestamp()
    )
    return df_final


def process_gold_airline_performance(spark):
    print("Loading AIRLINE data into Gold...")
    sys.stdout.flush()

    df_raw = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_fact_flights")
    df_dim_airlines = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_dim_airlines")

    df1 = add_business_features(df_raw)
    df2 = _join_airline(df1, df_dim_airlines)
    df_final = _group_by_date_and_airline(df2)

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.gold_airline_performance"
    composite_pk = ["scheduled_date", "airline_id"]

    delta_utils.upsert_to_delta(spark, df_final, target_table, composite_pk)

    print(f"Successfully loaded AIRLINE PERFORMANCE data into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    process_gold_airline_performance(spark)