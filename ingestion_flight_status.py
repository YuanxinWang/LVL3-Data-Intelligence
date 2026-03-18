import config
import json
import requests
import time
from datetime import datetime, timedelta


KEY_RESOURCE = "FlightStatusResource"
KEY_META     = "Meta"
KEY_TOTAL    = "TotalCount"

# return a list from today to lookback_days ago
# form: ['YYYY-MM-DD', 'YYYY-MM-DD', ...]
def get_date_list(lookback_days):
    date_list = []
    for i in range(lookback_days):
        date = datetime.now() - timedelta(days = i)
        date_list.append(date.strftime("%Y-%m-%d"))
    return date_list


# send out a request
# return the json if success, else return None
def single_fetch(pre_url, offset, limit):
    url = f"{pre_url}?limit={limit}&offset={offset}"
    try:
        response = requests.get(url, headers=config.HEADERS, timeout = 20)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# save json file to volume with offset and limit in name for tracking
def save_to_volume(raw_json, airport, flight_type, date_str, offset, limit):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{timestamp}_{airport}_{flight_type}_{date_str}_off{offset}_lim{limit}.json"
    full_path = f"{config.VOLUME_PATH}{filename}"

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)
    print(f"[{datetime.now()}] file saved: {filename}")


# binary search when fail
# extra return check for different search cases
def execute_ingestion_flight_status(pre_url, offset, limit, airport, flight_type, date_str):
    raw_data = single_fetch(pre_url, offset, limit)
    if raw_data is not None:
        save_to_volume(raw_data, airport, flight_type,date_str, offset, limit)
        return raw_data.get(KEY_RESOURCE, {}).get(KEY_META, {}).get(KEY_TOTAL, 0)
    else:
        if limit <= 1:
            print(f"    [Critical] Offset {offset} is broken. Skipping.")
            return None
        mid = limit // 2
        print(f"    [Retry] Offset {offset}, New Limit {mid}")
        res1 = execute_ingestion_flight_status(pre_url, offset, mid, airport, flight_type, date_str)
        res2 = execute_ingestion_flight_status(pre_url, offset + mid, limit - mid, airport, flight_type, date_str)
        if res1 is not None and res2 is not None:
            return max(res1, res2)
        return res1 if res1 is not None else res2


# In one 4 hour block, push offset forward until total count is reached
def fetch_window_paginated(pre_url, airport, flight_type, data_str):
    current_offset = 0
    total_count = 1
    
    while current_offset < total_count:
        returned_total = execute_ingestion_flight_status(
            pre_url, current_offset, config.API_LIMIT,
            airport, flight_type, data_str)
        if returned_total is not None and current_offset == 0:
            total_count = returned_total
            print(f"	[Total] {total_count} flights expected.")
        current_offset += config.API_LIMIT
        time.sleep(0.4)
