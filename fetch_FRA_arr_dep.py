import sys
import os

# Allow Python to look up files in this folder
sys.path.append(os.path.abspath('.'))
import config
import ingestion_flight_status as ingest

target_dates = ingest.get_date_list(config.LOOKBACK_DAYS)
time_slots = ["04:00", "08:00", "12:00", "16:00", "20:00", "00:00"]
flight_types = ["arrivals", "departures"]

for date in target_dates:
    print(f"Processing {date}")
    for slot in time_slots:
        from_datetime = f"{date}T{slot}"
        print(f"\nProcessing {from_datetime}")
        
        for flight_type in flight_types:
            print(f"Fetching {flight_type}...")
            
            full_url = f"{config.BASE_URL}/v1/operations/flightstatus/{flight_type}/{config.HUB_AIRPORT}/{from_datetime}"
            ingest.execute_ingestion_flight_status(full_url, 0, config.API_LIMIT, config.HUB_AIRPORT, flight_type, from_datetime.replace(":", ""))
