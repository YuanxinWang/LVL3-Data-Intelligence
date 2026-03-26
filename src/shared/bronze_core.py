import logging
from pyspark.sql.functions import current_timestamp


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# @dp.table instead of .writeStream and awaitTermination()
def get_bronze_stream(spark, source_path, job_description="Bronze Data"):
    """
    Reads JSON data using Databricks Auto Loader and returns the streaming DF
    """
    logger.info(f"[{job_description}] Starting Auto Loader...")
    logger.info(f"[{job_description}] Reading from: {source_path}")
    raw_df = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.inferColumnTypes", "true")
              .option("multiLine", "true")
              .load(source_path)
    )
    enriched_df = raw_df.withColumn("bronze_ingested_at", current_timestamp())
    logger.info(f"[{job_description}] Stream reading initialized successfully")
    return enriched_df
