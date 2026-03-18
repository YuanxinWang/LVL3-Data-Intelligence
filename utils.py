import json
import requests
from datetime import datetime, timedelta
import config


# send out a request
# return the json if success, else return None
def single_fetch(pre_url, offset, limit):
    url = f"{pre_url}?limit={limit}&offset={offset}"
    try:
        response = requests.get(url, headers=config.HEADERS, timeout = 20)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"[{datetime.now()} Request failed at offset {offset}: {e}]")
        return None


# save json file to volume with offset and limit in name for tracking
def save_to_volume(raw_json, target_path, filename):
    full_path = f"{target_path}{filename}"

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)
    print(f"[{datetime.now()}] file saved: {filename}")