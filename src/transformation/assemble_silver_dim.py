import sys
import os
from pyspark.sql.functions import broadcast,current_timestamp

current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils


# read the three tmp table from staging layer, join as the final silver airport table
def assemble_dim_airports(spark):
    print("Assembling final AIRPORTS dimension from tmp tables...")
    sys.stdout.flush()
    
    df_ap = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_airports")
    df_ct = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_cities")
    df_co = spark.read.table(f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_tmp_countries")

    final_airports = df_ap.join(
        	broadcast(df_ct), on="city_code", how="left"
        ).join(
            broadcast(df_co), on="country_code", how="left"
        ).drop(
            "silver_processed_at"
        ).withColumn(
            "silver_processed_at", current_timestamp()
        )

    target_table = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.silver_dim_airports"
    delta_utils.upsert_to_delta(spark, final_airports, target_table, "airport_code")
    print(f"Successfully assembled final {target_table}\n")
    sys.stdout.flush()


if __name__ == "__main__":
    assemble_dim_airports(spark)
