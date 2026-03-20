import os
import json
import requests
import time
from datetime import datetime, timedelta
from src.shared import config


# API known issue: Arrays containing just 1 member are not shown as arrays in JSON responses
# make sure we always have a list in the end
def ensure_list(element):
    if element is None:
        return []
    elif isinstance(element, list):
        return element
    return [element]


# Instead of hard coding item_key, run the last for loop to let it figured automatically
def normalize_data(data, resource_key):
    if not data or resource_key not in data:
        return data
    try:
        resource_node = data[resource_key]
        for collection_key, collection_value in resource_node.items():
            if collection_key == config.KEY_META:
                continue
            if isinstance(collection_value, dict):
                for item_key, item_value in collection_value.items():
                    collection_value[item_key] = ensure_list(item_value)
    except Exception as e:
        print(f"[{datetime.now()}] Warning: Generic normalization failed: {e}")
    return data


# print out the error details for API error responses.
def log_api_error(data, offset):
    error_node = data.get(config.KEY_ERROR_ROOT)
    if not error_node:
        return
    if isinstance(error_node, dict):
        error_node = error_node.get(config.KEY_ERROR_DETAILS, error_node)  
    error_list = ensure_list(error_node)
    if error_list:
        first_error = error_list[0]
        error_type = first_error.get(config.KEY_ERROR_TYPE, "Unknown")
        error_desc = first_error.get(config.KEY_ERROR_DESC, "No description")
        print(f"[{datetime.now()}] API Error at offset {offset}: {error_type} - {error_desc}")


# based on testing result, lots of times API respond 200 secceess code with fake data
# before save respond data into JSON, security check:
# process error: throw out for binary search to skip broken data / stop for empty data
# Connection error: call exponential backoff for retry
# normal data: pass back for saving
def validate_response(data, offset):
    if not isinstance(data, dict):
        return data
    if config.KEY_ERROR_ROOT not in data and config.KEY_META not in data:
        for err_key in config.PROXY_ERROR_KEYS:
            if err_key in data and data and data[err_key]:
                error_message = data[err_key]
                raise Exception(f"Proxy network issue (Fake 200 OK): {error_message}")
    if config.KEY_ERROR_ROOT in data:
        log_api_error(data, offset)
        return None
    return data


# send out a request
# return the json if success, else return None
# exponential backoff retry mechanism for robustness against transient API issues.
# add seperator logic to fit for both reference data and flight status data.
def single_fetch(pre_url, offset, limit):
    seperator = '&' if '?' in pre_url else '?'
    url = f"{pre_url}{seperator}limit={limit}&offset={offset}"
    
    for attempt in range(config.MAX_RETRIES):
        try:
            response = requests.get(url, headers=config.HEADERS, timeout = 20)
            if response.status_code == 200:
                return validate_response(response.json(), offset)
            elif response.status_code in [429, 502, 503, 504]:
                raise Exception(f"API Rate Limit or Server Error ({response.status_code})")
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
# create new folder if not exist, so it won't error out when saving files.
def save_to_volume(raw_json, target_path, filename):
    full_path = f"{target_path}{filename}"
    os.makedirs(target_path, exist_ok=True)
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, ensure_ascii=False, indent=4)
    print(f"[{datetime.now()}] file saved: {filename}")