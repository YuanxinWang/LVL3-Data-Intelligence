import sys
import os
from pyspark.sql.functions import current_timestamp
# not adding the following, might cause break down
# from pyspark.sql import SparkSession
# from pyspark.sql.funtions import current_timestamp


# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config


print(f"Starting Auto Loader: Bronze Flight Status")


def load_bronze_flights():
    source_path = config.VOLUME_FLIGHT_STATUS
    print(f"Reading from source path: {source_path}")
    raw_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", config.CHK_BRONZE_FLIGHTS)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("multiLine", "true")
              .load(source_path))
    enriched_df = raw_df.withColumn("bronze_ingested_at", current_timestamp())
    print(f"Writing to Delta Table: {config.TABLE_BRONZE_FLIGHTS}")
    query = (enriched_df.writeStream
             .format("delta")
             .option("checkpointLocation", config.CHK_BRONZE_FLIGHTS)
             .trigger(availableNow=True)
             .option("mergeSchema", "true")
             .toTable(config.TABLE_BRONZE_FLIGHTS))
    query.awaitTermination()
    print("Bronze FLight Status loaded successfully.")


if __name__ == "__main__":
    load_bronze_flights()


# import sys
# import os

# # Allow Python to find src folder
# current_dir = os.getcwd()
# project_root = os.path.abspath(os.path.join(current_dir, "../../"))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# from src.shared import config
# from src.shared import bronze_core

# if __name__ == "__main__":
#     bronze_core.load_bronze_table(
#         spark=spark, 
#         source_path=config.VOLUME_FLIGHT_STATUS,
#         checkpoint_location=config.CHK_BRONZE_FLIGHTS,
#         table_name=config.TABLE_BRONZE_FLIGHTS,
#         job_description="Bronze Flights"
#     )
