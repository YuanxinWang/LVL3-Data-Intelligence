import sys
import os
import logging
import databricks.pipelines as dp

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared.bronze_core import get_bronze_stream


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("Starting Unified Bronze Load Declarations for Reference Data...")


# -------------------------------------------------------------------------
# ARCHITECTURE NOTE: WHY NESTED FUNCTIONS (CLOSURES)?
# -------------------------------------------------------------------------
# We use a factory pattern (closure) here to dynamically generate multiple 
# @dp.table pipelines inside a for-loop. 
# 
# In Python, decorators must wrap distinct function objects. If we define a 
# single function outside and loop over it, Python's "late-binding" behavior 
# will cause all iterations to point to the same memory address, resulting 
# in only the last table being created. 
# 
# Nesting `generate_table` forces Python to allocate a fresh, isolated 
# function in memory for each API endpoint, allowing the Databricks engine 
# to register them as separate tables successfully.
# -------------------------------------------------------------------------
def create_reference_table(endpoint_key):
    @dp.table(
        name = f"reference_{endpoint_key}",
        comment = f"Raw bronze data for {endpoint_key.upper()}"
    )
    def generate_table():
        source_path = f"{config.VOLUME_REFERENCE}{endpoint_key}"
        bronze_reference = get_bronze_stream(
            spark=spark,
            source_path=source_path,
            job_description=f"Bronze Reference - {endpoint_key.upper()}"
        )
        return bronze_reference
    return generate_table


for data_type in config.REFERENCE_ENDPOINTS.keys():
    create_reference_table(data_type)
