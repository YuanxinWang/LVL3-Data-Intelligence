from pyspark.sql import functions as F
from src.shared import config


# convert timezone and add business fetures (tiem wave, weekend, delay status)
# for delay responsibility:
#   departure delay: directly use departure delay minutes & schedualed departure time
#   arrival delay: arrival delay - departure delay (if <0, set to 0) & scheduled arrival time
# lit(0) -> create a column with value 0, used for comparison
def add_business_features(df_raw):
    df_local = df_raw.withColumn(
        "local_scheduled_dep",
        F.from_utc_timestamp("scheduled_departure_utc", "Europe/Berlin")
    ).withColumn(
        "local_actual_dep",
        F.from_utc_timestamp("actual_departure_utc", "Europe/Berlin")
    ).withColumn(
        "local_scheduled_arr", F.from_utc_timestamp("scheduled_arrival_utc", "Europe/Berlin")
    ).withColumn(
        "local_actual_arr", F.from_utc_timestamp("actual_arrival_utc", "Europe/Berlin")
    )

    df_direction = df_local.withColumn(
        "direction",
        F.when(F.col("dep_airport_code") == config.HUB_AIRPORT, "Departure")
        .when(F.col("arr_airport_code") == config.HUB_AIRPORT, "Arrival")
        .otherwise("Unknown")
    )

    df_reference_time = df_direction.withColumn(
        "reference_time",
        F.when(F.col("direction") == "Arrival", F.col("local_scheduled_arr"))
        .otherwise(F.col("local_scheduled_dep"))
    )

    df_add = df_reference_time.withColumn(
        "scheduled_date",
        F.to_date(F.col("reference_time"))
    ).withColumn(
        "scheduled_hour",
        F.hour(F.col("reference_time"))
    ).withColumn(
        "is_morning_wave",
        F.when(F.col("scheduled_hour").between(6, 9), True).otherwise(False)
    ).withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("reference_time")).isin([1,7]), True).otherwise(False)
    )

    df_delay = df_add.withColumn(
        "dep_delay_minutes",
        F.when(
            F.col("local_actual_dep").isNotNull(),
            F.round((F.unix_timestamp(F.col("local_actual_dep")) - F.unix_timestamp(F.col("local_scheduled_dep"))) / 60)
        ).otherwise(0)
    ).withColumn(
        "arr_delay_minutes",
        F.when(
            F.col("local_actual_arr").isNotNull(),
            F.round((F.unix_timestamp(F.col("local_actual_arr")) - F.unix_timestamp(F.col("local_scheduled_arr"))) / 60)
        ).otherwise(0)
    )

    df_responsibility = df_delay.withColumn(
        "delay_minutes",
        F.when(
            F.col("direction") == "Departure", F.col("dep_delay_minutes")
        ).when(
            F.col("direction") == "Arrival", 
            F.greatest(F.col("arr_delay_minutes") - F.col("dep_delay_minutes"), F.lit(0))
        ).otherwise(0)
    )

    df_new = df_responsibility.withColumn(
        "status_category",
        F.when(F.col("flight_status_desc") == "Cancelled", "Cancelled")
        .when(F.col("delay_minutes") <= 0, "On_time")
        .when(F.col("delay_minutes") < 15, "Minor_Delay")
        .otherwise("Major_Delay")
    )

    return df_new