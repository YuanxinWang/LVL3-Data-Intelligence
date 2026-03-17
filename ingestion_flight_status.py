import config
import json
import requests
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


def get_time_slots():
    return ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00"]


# send out a request
# return the json if success, else return None
def single_fetch(url, offset, limit):
    params = {"offset": offset, "limit": limit}
    try:
        response = requests.get(url, headers=config.HEADERS, timeout = 20)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# save json file to volume with offset and limit in name for tracking
def save_to_volume(raw_json, airport, date_str, offset, limit):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{airport}_{date_str}_off{offset}_lim{limit}_{timestamp}.json"
    full_path = f"{config.VOLUME_PATH}{filename}"

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)
    print(f"[{datetime.now()}] file saved: {filename}")


# binary search when fail
# extra return check for different search cases
def execute_ingestion_flight_status(pre-url, offset, limit, airport, date_str):
    raw_data = single_fetch(pre-url, offset, limit)
    if raw_data is not None:
        save_to_volume(raw_data, airport, date_str, offset, limit)
        return raw_data.get(KEY_RESOURCE, {}).get(KEY_META, {}).get(KEY_TOTAL, 0)
    else:
        if limit <= 1:
            print(f"    [Critical] Offset {offset} is broken. Skipping.")
            return None
        mid = limit // 2
        print(f"    [Retry] Offset {offset}, New Limit {mid}")
        res1 = execute_ingestion_flight_status(url, offset, mid, airport, date_str)
        res2 = execute_ingestion_flight_status(url, offset + mid, limit - mid, airport, date_str)
        if res1 is not None and res2 is not None:
            return max(res1, res2)
        return res1 if res1 is not None else res2
