# without Databricks env, dbtils will error.
# Program will try to take local env instead.
# Call ov.getenv() in this case - need os library
import os
from datetime import datetime

BASE_URL = "https://lh-proxy.onrender.com"

# Fallback setup to enable local testing without Databricks env.
try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    PROXY_PASSWORD = dbutils.secrets.get(scope="lh-proxy", key="password")
except:
    PROXY_PASSWORD = os.getenv("LH_PROXY_PASSWORD", "default_password")

HEADERS = {"password": PROXY_PASSWORD}
VOLUME_PATH = "/Volumes/workspace/lufthansa/raw_data/"
HUB_AIRPORT = "FRA"
LOOKBACK_DAYS = 7
API_LIMIT = 100

print(f"[{datetime.now()}] Config loaded successfully. Proxy password: {'*' * len(PROXY_PASSWORD)}")