from pyspark.sql import functions as F
from src.shared import config


# convert timezone and add business fetures (tiem wave, weekend, delay status)\
def _convert_timezone(df):
    df_convert_timezone = df.withColumn(
        "local_scheduled_dep",
        F.from_utc_timestamp("scheduled_departure_utc", config.HUB_TIMEZONE)
    ).withColumn(
        "local_actual_dep",
        F.from_utc_timestamp("actual_departure_utc", config.HUB_TIMEZONE)
    ).withColumn(
        "local_scheduled_arr", F.from_utc_timestamp("scheduled_arrival_utc", config.HUB_TIMEZONE)
    ).withColumn(
        "local_actual_arr", F.from_utc_timestamp("actual_arrival_utc", config.HUB_TIMEZONE)
    )
    return df_convert_timezone


def _add_direction(df):
    df_dir = df.withColumn(
        "direction",
        F.when(F.col("dep_airport_code") == config.HUB_AIRPORT, "Departure")
        .when(F.col("arr_airport_code") == config.HUB_AIRPORT, "Arrival")
        .otherwise("Unknown")
    )
    return df_dir


# for scheduled time: only consider schedule at target airport
def _schedule_at_this_hub(df):
    df_ref = df.withColumn(
        "reference_time",
        F.when(F.col("direction") == "Arrival", F.col("local_scheduled_arr"))
        .otherwise(F.col("local_scheduled_dep"))
    )

    df_schedule_hub = df_ref.withColumn(
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
    return df_schedule_hub


# for delay responsibility:
#   departure delay: directly use departure delay minutes & schedualed departure time
#   arrival delay: arrival delay - departure delay (if <0, set to 0) & scheduled arrival time
# lit(0) -> create a column with value 0, used for comparison
def _calculate_delay(df):
    df_delay = df.withColumn(
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
    return df_responsibility


def _assign_status_category(df):
    df_status = df.withColumn(
        "status_category",
        F.when(F.col("flight_status_desc") == "Cancelled", "Cancelled")
        .when(F.col("delay_minutes") <= 0, "On_time")
        .when(F.col("delay_minutes") < 15, "Minor_Delay")
        .otherwise("Major_Delay")
    )
    return df_status


# here we only consider flights that has clear status results (landed / canclled)
def _filter_valid_flights(df):
    df_filtered = df.filter(
        (F.col("status_category") == "Cancelled")
        | ((F.col("direction") == "Departure") & F.col("local_actual_dep").isNotNull())
        | ((F.col("direction") == "Arrival") & F.col("local_actual_arr").isNotNull())
    )
    return df_filtered


def add_business_features(df_raw):
    df1 = _convert_timezone(df_raw)
    df2 = _add_direction(df1)
    df3 = _schedule_at_this_hub(df2)
    df4 = _calculate_delay(df3)
    df5 = _assign_status_category(df4)
    df_final = _filter_valid_flights(df5)

    return df_final
