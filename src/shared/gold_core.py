from pyspark.sql import functions as F


# convert timezone and add business fetures (tiem wave, weekend, delay status)
def add_business_features(df_raw):
    df_local = df_raw.withColumn(
        "local_scheduled_time",
        F.from_utc_timestamp("scheduled_departure_utc", "Europe/Berlin")
    ).withColumn(
        "local_actual_time",
        F.from_utc_timestamp("actual_departure_utc", "Europe/Berlin")
    )

    df_add = df_local.withColumn(
        "scheduled_date",
        F.to_date(F.col("local_scheduled_time"))
    ).withColumn(
        "scheduled_hour",
        F.hour(F.col("local_scheduled_time"))
    ).withColumn(
        "is_morning_wave",
        F.when(F.col("scheduled_hour").between(6, 9), True).otherwise(False)
    ).withColumn(
        "is_weekend",
        F.when(F.dayofweek(F.col("local_scheduled_time")).isin([1,7]), True).otherwise(False)
    )

    df_delay = df_add.withColumn(
        "delay_minutes",
        F.when(
            F.col("local_actual_time").isNotNull(),
            F.round((F.unix_timestamp(F.col("local_actual_time")) - F.unix_timestamp(F.col("local_scheduled_time"))) / 60)
        ).otherwise(0)
    )

    df_new = df_delay.withColumn(
        "status_category",
        F.when(F.col("flight_status_desc") == "Cancelled", "Cancelled")
        .when(F.col("delay_minutes") <= 0, "On_time")
        .when(F.col("delay_minutes") < 15, "Minor_Delay")
        .otherwise("Major_Delay")
    )

    return df_new