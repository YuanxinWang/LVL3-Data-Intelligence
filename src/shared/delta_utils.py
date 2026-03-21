from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from typing import Union, List


# helper function to construct the SQL-like merge condition.
def _build_merge_condition(primary_key: Union[str, List[str]]) -> str:
    if isinstance(primary_key, str):
        return f"target.{primary_key} = source.{primary_key}"
    conditions = []
    for key in primary_key:
        conditions.append(f"target.{key} = source.{key}")
    return " AND ".join(conditions)


# SCD Type 1: Overwrite
# with List support both single PK and composite PK
# Unity Catalog 3-level naming
# ///df: DataFrame. "Super Excel"///
def upsert_to_delta(spark: SparkSession, df: DataFrame, table_name: str, primary_key: Union[str, List[str]]):
    if not spark.catalog.tableExists(table_name):
        print(f"[{datetime.now()}] Table {table_name} does not exist. Initializing...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    else:
        print(f"[{datetime.now()}] Updating: Table {table_name} found. Executing Delta Merge (Upsert)...")
        delta_table = DeltaTable.forName(spark, table_name)
        merge_condition = _build_merge_condition(primary_key)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
