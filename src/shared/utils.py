import json
import requests
import time
from datetime import datetime, timedelta
import config


# send out a request
# return the json if success, else return None
def single_fetch(pre_url, offset, limit):
    url = f"{pre_url}?limit={limit}&offset={offset}"
    
    for attempt in range(config.MAX_RETRIES):
        try:
            response = requests.get(url, headers=config.HEADERS, timeout = 20)
            if response.status_code == 200:
                data = response.json()
                if config.KEY_ERROR_ROOT in data:
                    error_info = data[config.KEY_ERROR_ROOT].get(config.KEY_ERROR_DETAILS, {})
                    error_type = error_info.get(config.KEY_ERROR_TYPE, "Unknown")
                    error_desc = error_info.get(config.KEY_ERROR_DESC, "No description")
                    print(f"[{datetime.now()}] API Error at offset {offset}: {error_type} - {error_desc}")
                    return None
                return data
            elif response.status_code in [429, 502, 503, 504]:
                delay = config.BASE_DELAY * (2 ** attempt)
                print(f"[{datetime.now()}] API Rate Limit. Retrying in {delay}s. ({attempt+1}/{config.MAX_RETRIES})")
                time.sleep(delay)
                continue
            else:
                print(f"[{datetime.now()}] Error: {response.status_code} at offset {offset}")
                return None
        except Exception as e:
            delay = config.BASE_DELAY * (2 ** attempt)
            print(f"[{datetime.now()}] Request Exception: {e}. Retrying in {delay}s. ({attempt+1}/{config.MAX_RETRIES})")
            time.sleep(delay)
    print(f"[{datetime.now()}] Max retries reached for offset {offset}. Giving up.")
    return None


# save json file to volume with offset and limit in name for tracking
def save_to_volume(raw_json, target_path, filename):
    full_path = f"{target_path}{filename}"
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)
    print(f"[{datetime.now()}] file saved: {filename}")