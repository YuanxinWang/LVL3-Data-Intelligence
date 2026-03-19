# without Databricks env, dbtils will error.
# Program will try to take local env instead.
# Call ov.getenv() in this case - need os library
# In our case, provide a default value for testing purposes.
import os
from datetime import datetime

BASE_URL = "https://lh-proxy.onrender.com"

# Fallback setup to enable local testing without Databricks env.
try:
    from databricks.sdk.runtime import dbutils

    PROXY_PASSWORD = dbutils.secrets.get(scope="lh_secrets", key="proxy_pass")
except Exception as e:
    print(f"[{datetime.now()}] Databricks secret fetch failed: {e}")
    PROXY_PASSWORD = os.getenv("LH_PROXY_PASSWORD", "default_password")

HEADERS = {"password": PROXY_PASSWORD}

VOLUME_BASE_PATH = "/Volumes/workspace/lufthansa/raw_data"
VOLUME_FLIGHT_STATUS = f"{VOLUME_BASE_PATH}/flight_status/"
VOLUME_REFERENCE = f"{VOLUME_BASE_PATH}/reference_data/"

HUB_AIRPORT = "FRA"
LOOKBACK_DAYS = 7
FLI_API_LIMIT = 50
REF_API_LIMIT = 100

MAX_RETRIES = 12
BASE_DELAY = 2

KEY_ERROR_ROOT = "ProcessingErrors"
KEY_ERROR_DETAILS = "ProcessingError"
KEY_ERROR_TYPE = "Type"
KEY_ERROR_DESC = "Description"

KEY_META = "Meta"
KEY_TOTAL = "TotalCount"

TIME_SLOTS = ["04:00", "08:00", "12:00", "16:00", "20:00", "00:00"]
FLIGHT_TYPES = ["arrivals", "departures"]

print(f"[{datetime.now()}] Config loaded successfully. Proxy password: {'*' * len(PROXY_PASSWORD)}")