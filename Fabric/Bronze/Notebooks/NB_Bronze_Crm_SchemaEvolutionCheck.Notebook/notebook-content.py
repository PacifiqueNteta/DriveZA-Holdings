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
source_table_name = ""
schema_name = ""
destination_table_name = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Logic cell ──
from datetime import datetime
import json

bronze_table = f"{schema_name}.{destination_table_name}"
changes = {"new_columns": [], "removed_columns": [], "type_changes": []}
CHANGE_LOG_TABLE = "metadata.schema_change_log"

if not spark.catalog.tableExists(bronze_table):
    print(f"{bronze_table} does not exist yet — first load, nothing to compare.")
    mssparkutils.notebook.exit("no_drift_first_load")

# Placeholder read — replace with a lightweight schema-only read against
# the actual source connection (e.g. LIMIT 0 query via JDBC).
source_df = spark.read.table(f"source_{schema_name}.{source_table_name}")

existing_schema = {f.name: f.dataType.simpleString() for f in spark.table(bronze_table).schema.fields}
incoming_schema = {f.name: f.dataType.simpleString() for f in source_df.schema.fields}

for col, dtype in incoming_schema.items():
    if col not in existing_schema:
        changes["new_columns"].append((col, dtype))

for col in existing_schema:
    if col not in incoming_schema:
        changes["removed_columns"].append(col)

for col in incoming_schema:
    if col in existing_schema and existing_schema[col] != incoming_schema[col]:
        changes["type_changes"].append((col, existing_schema[col], incoming_schema[col]))

log_rows = []
now = datetime.now().isoformat()

for col, dtype in changes["new_columns"]:
    spark.sql(f"ALTER TABLE {bronze_table} ADD COLUMN {col} {dtype}")
    print(f"Added column {col} ({dtype}) to {bronze_table}")
    log_rows.append((schema_name, source_table_name, destination_table_name,
                      "new_column", col, None, dtype, now, True, False))

for col in changes["removed_columns"]:
    print(f"MANUAL REVIEW REQUIRED — column removed from source: {col}")
    log_rows.append((schema_name, source_table_name, destination_table_name,
                      "removed_column", col, existing_schema.get(col), None, now, False, True))

for col, old_type, new_type in changes["type_changes"]:
    print(f"MANUAL REVIEW REQUIRED — type change on {col}: {old_type} -> {new_type}")
    log_rows.append((schema_name, source_table_name, destination_table_name,
                      "type_change", col, old_type, new_type, now, False, True))

if log_rows:
    log_df = spark.createDataFrame(
        log_rows,
        schema=["schema_name", "table_name", "destination_table_name", "change_type",
                "column_name", "old_type", "new_type", "detected_at",
                "applied_automatically", "requires_manual_review"]
    )
    log_df.write.format("delta").mode("append").saveAsTable(CHANGE_LOG_TABLE)
    print(f"Logged {len(log_rows)} schema change(s) to {CHANGE_LOG_TABLE}")
else:
    print("No schema drift detected — nothing to log.")

mssparkutils.notebook.exit(json.dumps(changes))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
