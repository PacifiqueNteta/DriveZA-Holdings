# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7ef943d7-63c2-467c-bf59-4f02a6e675f3",
# META       "default_lakehouse_name": "LH_DRZ_BRONZE",
# META       "default_lakehouse_workspace_id": "a93dcff1-e562-4cf4-9954-8e1176cfc71c",
# META       "known_lakehouses": [
# META         {
# META           "id": "7ef943d7-63c2-467c-bf59-4f02a6e675f3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Logic cell

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime, timezone

run_start = datetime.now(timezone.utc).isoformat()

if not table_name:
    mssparkutils.notebook.exit(f"{default_watermark}|{initial_load_column}|{run_start}|")

CTRL_TABLE = "metadata.pipeline_control"

SOURCE_TYPE = "sqlserver"
SOURCE_SYSTEM = "CRM"

row = (
    spark.table(CTRL_TABLE)
    .filter((F.col("table_name") == table_name) 
    & (F.col("schema_name") == schema_name))
    .select("max_incremental_value")
    .collect()
)

if row and row[0]["max_incremental_value"] is not None:
    watermark = row[0]["max_incremental_value"]
    effective_column = incremental_column
else:
    watermark = default_watermark
    effective_column = initial_load_column

# Ready-to-use SQL query (CPY_Incrementalwill consume this as one plain string, no pipeline-side concat() needed.)
incremental_query = f"SELECT * FROM [{schema_name}].{table_name} WHERE {effective_column} > '{watermark}'"

print(f"[{schema_name}].[{table_name}] → column={effective_column}, watermark={watermark}")
print(f"Built query: {incremental_query}")

mssparkutils.notebook.exit(f"{watermark}|{effective_column}|{run_start}|{incremental_query}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
