from pyspark.sql.functions import current_timestamp
from datetime import datetime


def load_bronze_table(spark, source_path, checkpoint_path, table_name, job_description="Bronze Data"):
    print(f"[{datetime.now()}] [{job_description}] Starting Auto Loader...")
    print(f"[{datetime.now()}] [{job_description}] Reading from: {source_path}")
    raw_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", checkpoint_path)
              .option("cloudFiles.inferColumnTypes", "true")
              .option("multiLine", "true")
              .load(source_path)
    )
    enriched_df = raw_df.withColumn("bronze_ingested_at", current_timestamp())
    print(f"[{datetime.now()}] [{job_description}] Writing to Delta Table: {table_name}")
    query = (enriched_df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .trigger(availableNow=True)
             .option("mergeSchema", "true")
             .toTable(table_name)
    )
    query.awaitTermination()
    print(f"[{datetime.now()}] [{job_description}] Loaded successfully.")
