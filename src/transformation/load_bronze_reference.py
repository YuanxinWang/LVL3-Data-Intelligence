import sys
import os

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import bronze_core


if __name__ == "__main__":
    print(f"Starting Unified Bronze Load for Reference Data...")
    for data_type in config.REFERENCE_ENDPOINTS.keys():
        source_path = f"{config.VOLUME_REFERENCE}{data_type}/"
        checkpoint_path = f"{config.CHECKPOINT_BASE}/bronze_reference_{data_type}/"
        table_name = f"{config.CATALOG_NAME}.{config.SCHEMA_NAME}.bronze_reference_{data_type}"
        print(f"Loading {data_type.upper()} reference data into Bronze...")
        bronze_core.load_bronze_table(
            spark=spark, 
            source_path=source_path,
            checkpoint_path=checkpoint_path,
            table_name=table_name,
            job_description=f"Bronze Reference - {data_type.upper()}"
        )
    print("All reference data loaded into Bronze successfully!")
