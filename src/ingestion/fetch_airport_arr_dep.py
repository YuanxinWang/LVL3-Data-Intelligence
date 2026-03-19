import sys
import os
from datetime import datetime, timedelta

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import ingestion_core


# Utility function to generate list of target dates based on lookback configuration
def get_date_list(lookback_days):
    date_list = []
    for i in range(lookback_days):
        date = datetime.now() - timedelta(days = i)
        date_list.append(date.strftime("%Y-%m-%d"))
    return date_list


target_dates = get_date_list(config.LOOKBACK_DAYS)


# Iterate through each date, time slot, and flight type to fetch flight status data
for date in target_dates:
    for slot in config.TIME_SLOTS:
        from_datetime = f"{date}T{slot}"
        for flight_type in config.FLIGHT_TYPES:
            print(f"Fetching {flight_type.upper()} for {from_datetime}...")
            
            pre_url = f"{config.BASE_URL}/v1/operations/flightstatus/{flight_type}/{config.HUB_AIRPORT}/{from_datetime}"
            file_prefix = f"{config.HUB_AIRPORT}_{flight_type}_{from_datetime.replace(':', '')}"
            resource_key = "FlightStatusResource"

            ingestion_core.fetch_paginated(
                pre_url=pre_url, 
                file_prefix=file_prefix, 
                resource_key=resource_key, 
                target_path=config.VOLUME_FLIGHT_STATUS,
                api_limit=config.FLI_API_LIMIT
            )

print("Flight Status Ingestion Completed Successfully!")