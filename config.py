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
VOLUME_PATH = "/Volumes/workspace/lufthansa/raw_data/"
HUB_AIRPORT = "FRA"
LOOKBACK_DAYS = 7
API_LIMIT = 100

print(f"[{datetime.now()}] Config loaded successfully. Proxy password: {'*' * len(PROXY_PASSWORD)}")