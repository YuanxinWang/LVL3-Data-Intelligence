import sys
import os
from pyspark.sql.functions import current_timestamp

# Allow Python to find src folder
sys.path.append(os.path.abspath('.'))
from src.shared import config


print(f"Starging Auto Loader: Bronze Flight Status")


def load_bronze_flight():
    source_path = config.VOLUME_FLIGHT_STATUS
    print(f"Reading from source path: {source_path}")
    raw_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", config.CHK_BRONZE_FLIGHTS)
              .option("cloudFiles.inferColumnTypes", "true")
              .load(source_path))
    enriched_df = raw_df.withColumn("bronze_intested_at", current_timestamp())
    print(f"Writing to Delta Table: {config.TABLE_BRONZE_FLIGHTS}")
    query = (enriched_df.writeStream
             .format("delta")
             .option("checkpointLocation", config.CHK_BRONZE_FLIGHTS)
             .trigger(availableNow=True)
             .option("mergeSchema", "true")
             .start(config.TABLE_BRONZE_FLIGHTS))
    query.awaitTermination()
    print("Bronze FLight Status loaded successfully.")
    