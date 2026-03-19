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

# Volume paths for saving raw JSON files
VOLUME_BASE_PATH = "/Volumes/workspace/lufthansa/raw_data"
VOLUME_FLIGHT_STATUS = f"{VOLUME_BASE_PATH}/flight_status/"
VOLUME_REFERENCE = f"{VOLUME_BASE_PATH}/reference_data/"

# Target airport and lookback configuration
HUB_AIRPORT = "FRA"
LOOKBACK_DAYS = 7

# API limits for different endpoints
FLI_API_LIMIT = 50
REF_API_LIMIT = 100

# Exponential backoff configuration
MAX_RETRIES = 12
BASE_DELAY = 2

# Error handling keys based on API response structure
KEY_ERROR_ROOT = "ProcessingErrors"
KEY_ERROR_DETAILS = "ProcessingError"
KEY_ERROR_TYPE = "Type"
KEY_ERROR_DESC = "Description"

# Metadata keys to read total count for pagination
KEY_META = "Meta"
KEY_TOTAL = "TotalCount"

# Time slots and flight types for fetching flight status data
TIME_SLOTS = ["04:00", "08:00", "12:00", "16:00", "20:00", "00:00"]
FLIGHT_TYPES = ["arrivals", "departures"]

print(f"[{datetime.now()}] Config loaded successfully. Proxy password: {'*' * len(PROXY_PASSWORD)}")


# ----- Medallion Architecture Configurations -----

# Unity Catalog standard name: catalog.schema.table
CATALOG_NAME = "workspace"
SCHEMA_NAME = "lufthansa"

# Checkpoint path
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
CHK_BRONZE_FLIGHTS = f"{CHECKPOINT_BASE}/bronze_flight_status/"

# Table names
TABLE_BRONZE_FLIGHTS = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_flight_status"
