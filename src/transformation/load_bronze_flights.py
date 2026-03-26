import sys
import os
import logging
import databricks.pipelines as dp
# not adding the following, might cause break down
# from pyspark.sql import SparkSession
# from pyspark.sql.funtions import current_timestamp


# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared.bronze_core import get_bronze_stream

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dp.table(
    name = "flight_status",
    comment = "Raw streaming flight status data ingested from JSON volumes"
)


def bronze_flight_status():
    logger.info("Starging Auto Loader Pipeline: Bronze Flight Status")
    bronze_flight = get_bronze_stream(
        spark=spark,
        source_path=config.VOLUME_FLIGHT_STATUS,
        job_description="Bronze Flight Status"
        )
    return bronze_flight
