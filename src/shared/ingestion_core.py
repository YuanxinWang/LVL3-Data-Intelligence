import config
import utils
import time
from datetime import datetime


# Core function to execute paginated fetching with error handling and binary search retry mechanism
def execute_ingestion(pre_url, offset, limit, file_prefix, resource_key, target_path):
    raw_data = utils.single_fetch(pre_url, offset, limit)
    if raw_data is not None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{timestamp}_{file_prefix}_off{offset}_lim{limit}.json"

        utils.save_to_volume(raw_data, target_path, filename)
        return raw_data.get(resource_key, {}).get(config.KEY_META, {}).get(config.KEY_TOTAL, 0)
    else:
        if limit <= 1:
            print(f"	[Critical] Offset {offset} for {file_prefix} is broken. Skipping.")
            return None
        mid = limit // 2
        print(f"	[Retry] Binary Search at Offset {offset}, New Limit {mid}")

        res1 = execute_ingestion(pre_url, offset, mid, file_prefix, resource_key, target_path)
        res2 = execute_ingestion(pre_url, offset + mid, limit - mid, file_prefix, resource_key, target_path)
    
        if res1 is not None and res2 is not None:
            return max(res1, res2)
        return res1 if res1 is not None else res2


# Wrapper function to handle pagination logic
def fetch_paginated(pre_url, file_prefix, resource_key, target_path, api_limit):
    current_offset = 0
    total_count = 1

    while current_offset < total_count:
        returned_total = execute_ingestion(
            pre_url, current_offset, api_limit, file_prefix, resource_key, target_path
        )
        if returned_total is not None and current_offset == 0:
            total_count = returned_total
            print(f"    [Total] {total_count} records expected for {file_prefix}.")
        current_offset += api_limit
        time.sleep(0.5)
