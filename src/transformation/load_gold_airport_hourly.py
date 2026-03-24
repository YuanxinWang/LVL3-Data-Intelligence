import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils
from src.shared import gold_core


# group by date and hour
# group: build folders: date & hour -> data now sorted
# gropBy("scheduled_date", "scheduled_hour"): new key （columns）: scheduled_date & scheduled_hour ...
# agg: caulculator -> how many on time, how long total delayed. (always couple with groupBy, served as withColumn)
# (withColumn keeps current, agg creates from scratch)
# still leave is_weekend check and is_morning_wave check for dashboard design (used as filter)
# afterwards, the new chart will be like:
# scheduled_date | scheduled_hour | direction | is_weekend | is_morning_wave | total_flights | on_time_flights
# | cancelled_flights | total_delay_minutes | avg_delay_minutes
def _group_by_date_and_hour(df_added):
    df_group = df_added.groupBy(
        "scheduled_date",
        "scheduled_hour",
        "direction",
        "is_weekend",
        "is_morning_wave",
    ).agg(
        F.count("flight_number").alias("total_flights"),
        F.sum(F.when(F.col("status_category") == "On_time", 1).otherwise(0)).alias("on_time_flights"),
        F.sum(F.when(F.col("status_category") == "Cancelled", 1).otherwise(0)).alias("cancelled_flights"),
        F.sum("delay_minutes").alias("total_delay_minutes"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes")
    )

    return df_group


# Ripple Effect: check for past 3 hoours
# window: look at particular range
# partitionBy("scheduled_date"): only look at the same date, delay of previous day should not impact the next day
# orderBy("scheduled_hour"): make sure data is sorted by hour, 0, 1, 2, ... so we can look at the past 3 hours
# rangeBetween(-3, -1): own position is 0, look at past 3 hours, so -3, -2, -1
# care: 
#   rowBetween is based on physical position, so if there is missing hour, it will still look at 3 rows,
#   which may not be the past 3 hours.
#   rangeBetween is based on the value of the orderBy column, so if there is missing hour,
#   it will calculate the value and stay focus on three hous only.
# if first flight in the morning, no past 3 hours, so fill with 0
def _past_three_hours_delay(df_group):
    my_window = Window.partitionBy("scheduled_date", "direction").orderBy("scheduled_hour").rangeBetween(-3, -1)

    df_final = df_group.withColumn(
        "past_3_hours_delay",
        F.sum("total_delay_minutes").over(my_window)
    ).fillna(
        {"past_3_hours_delay": 0}
    )

    df_with_timestamp = df_final.withColumn(
        "gold_processed_at", F.current_timestamp()
    )

    return df_with_timestamp


def process_gold_airport_hourly(spark):
    print("Loading AIRPORT HOURLY data into Gold...")
    sys.stdout.flush()

    df_raw = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_fact_flights")
    df1 = gold_core.add_business_features(df_raw)
    df2 = _group_by_date_and_hour(df1)
    df_final = _past_three_hours_delay(df2)

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.gold_airport_hourly_operations"
    composite_pk = ["scheduled_date", "scheduled_hour", "direction"]
    delta_utils.upsert_to_delta(spark, df_final, target_table, composite_pk)

    print(f"Successfully loaded AIRPORT HOURLY data into {target_table}")
    sys.stdout.flush()


if __name__ == "__main__":
    process_gold_airport_hourly(spark)
