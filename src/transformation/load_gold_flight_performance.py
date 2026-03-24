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
from src.shared import gold_core


# group by flight number and airline id
# group: build folders: date & hour -> data now sorted
# agg: always couple with groupBy, served as withColumn (withColumn keeps current, agg creates from scratch)
# afterwards, the new chart will be like:
# flight_number | direction | airline_id | total_flights | cancelled_times | avg_delay_minutes
# | max_delay_minutes | on_time_times | on_time_rate | gold_processed_at
def _group_by_flight_number(df_added):
    df_group = df_added.groupBy(
        "flight_number",
        "direction",
        "airline_id"
    ).agg(
        F.count("flight_number").alias("total_flights"),
        F.sum(F.when(F.col("status_category") == "Cancelled", 1).otherwise(0)).alias("cancelled_times"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
        F.max("delay_minutes").alias("max_delay_minutes"),
        F.sum(F.when(F.col("status_category") == "On_time", 1).otherwise(0)).alias("on_time_times")
    )

    df_final = df_group.withColumn(
        "on_time_rate",
        F.round((F.col("on_time_times") / F.col("total_flights")) * 100, 2)
    ).withColumn(
        "gold_processed_at", F.current_timestamp()
    )

    return df_final


def process_gold_flight_performance(spark):
    print("Loading FLIGHT PERFORMANCE data into Gold...")
    sys.stdout.flush()

    df_raw = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_fact_flights")
    df_business = gold_core.add_business_features(df_raw)
    df_final = _group_by_flight_number(df_business)

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.gold_flight_performance"
    composite_pk = ["flight_number", "direction", "airline_id"]

    delta_utils.upsert_to_delta(spark, df_final, target_table, composite_pk)

    print(f"Successfully loaded FLIGHT PERFORMANCE data into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    process_gold_flight_performance(spark)
