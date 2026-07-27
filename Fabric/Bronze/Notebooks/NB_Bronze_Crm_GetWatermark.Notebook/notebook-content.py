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

# CELL ********************

# ── Parameters cell ──
table_name = ""
schema_name = ""
default_watermark = ""
initial_load_column = ""
incremental_column = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Logic cell ──
from pyspark.sql import functions as F

if not table_name:
    mssparkutils.notebook.exit(f"{default_watermark}|{initial_load_column}")

CTRL_TABLE = "metadata.pipeline_watermark"

# No table-creation logic here anymore — NB_Bronze_Meta_Setup guarantees
# this table exists before any pipeline runs. If it's missing, this will
# correctly fail loudly rather than silently self-healing, which is the
# right behavior: a missing control table means setup wasn't run, and
# that should surface as an error, not be papered over mid-pipeline.

row = (
    spark.table(CTRL_TABLE)
    .filter((F.col("table_name") == table_name) & (F.col("schema_name") == schema_name))
    .select("max_incremental_value")
    .collect()
)

if row and row[0]["max_incremental_value"] is not None:
    watermark = row[0]["max_incremental_value"]
    effective_column = incremental_column
else:
    watermark = default_watermark
    effective_column = initial_load_column

print(f"[{schema_name}].[{table_name}] → column={effective_column}, watermark={watermark}")

mssparkutils.notebook.exit(f"{watermark}|{effective_column}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
