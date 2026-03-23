import sys
import os
from pyspark.sql import functions as F

# Allow Python to find src folder
current_dir = os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.shared import config
from src.shared import delta_utils
from src.shared.gold_core import add_business_features


def _join_airline(df_new, df_dim_airline):
    