from databricks.connect import DatabricksSession

print("Testing: Calling Databricks Serverless...")

# .serverless()，forcing the serverless execution
spark = DatabricksSession.builder.serverless().getOrCreate()

print("Cloud Serverless connected. Sending computation task...")

# run a simple SQL query to test the connection and computation
df = spark.sql("SELECT 'It works.' AS Result")

# pull the computation results back to the local environment for display
df.show()