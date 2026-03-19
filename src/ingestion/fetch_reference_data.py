import sys
import os

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import ingestion_core


REFERENCE_ENDPOINTS = {
    "airports": "AirportResource",
    "cities": "CityResource",
    "countries": "CountryResource",
    "airlines": "AirlineResource",
    "aircraft": "AircraftResource"
}


for data_type, resource_key in REFERENCE_ENDPOINTS.items():
    print(f"\nProcessing {data_type.upper()}...")
    pre_url = f"{config.BASE_URL}/v1/references/{data_type}"
    file_prefix = f"ref_{data_type}"

    ingestion_core.fetch_paginated(
        pre_url=pre_url, 
        file_prefix=file_prefix, 
        resource_key=resource_key, 
        target_path=config.VOLUME_REFERENCE,
        api_limit=config.REF_API_LIMIT
    )


print("[Success] Reference Data Ingestion Completed Successfully!\n")