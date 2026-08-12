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

from datetime import datetime
import uuid

CTRL_CONTROL = "metadata.pipeline_control"
CTRL_RUN_LOG = "metadata.pipeline_run_log"

run_start = (datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
if start_time_str
else datetime.now(timezone.utc))
run_end = datetime.now(timezone.utc)
duration_seconds = int((run_end - run_start).total_seconds())

spark.sql(f"""
    UPDATE {CTRL_WATERMARK}
    SET 
        source_type = '{SOURCE_TYPE}',
        source_system = '{SOURCE_SYSTEM}',
        last_run_status = 'Failed',
        last_run_end_time = current_timestamp(),
        updated_at = current_timestamp()
    WHERE source_system = '{SOURCE_SYSTEM}' AND table_name = '{table_name}' AND schema_name = '{schema_name}' 
""")

safe_error = error_message.replace("'", "''")

spark.sql(f"""
    INSERT INTO {CTRL_RUN_LOG}
    (pipeline_run_id, pipeline_name, source_type, source_system, schema_name, table_name, destination_table_name, load_type,
     run_status, rows_read, rows_written, error_message, start_time, end_time, duration_seconds)
    VALUES (
        '{pipeline_run_id}', '{pipeline_name}', '{SOURCE_TYPE}', '{SOURCE_SYSTEM}', '{schema_name}', '{table_name}', '{destination_table_name}', '{load_type}',
        'Failed', 0, 0, '{safe_error}',
        '{run_start.isoformat()}', '{run_end.isoformat()}', {duration_seconds}
    )
""")

print(f"FAILURE recorded for [{schema_name}].[{table_name}]: {error_message}")
mssparkutils.notebook.exit("failure_recorded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
