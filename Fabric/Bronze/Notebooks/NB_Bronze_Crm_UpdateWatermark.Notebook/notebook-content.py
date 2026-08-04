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

# ── Logic cell ──
from pyspark.sql import functions as F
from datetime import datetime, timezone



CTRL_WATERMARK = "metadata.pipeline_watermark"
CTRL_RUN_LOG = "metadata.pipeline_run_log"
bronze_table = f"{schema_name}.{destination_table}"

run_start = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
run_end = datetime.now(timezone.utc)
duration_seconds = int((run_end - run_start).total_seconds())

rows_copied = spark.table(bronze_table).count()

if load_type.lower() == "incremental":

    new_watermark_row = (
        spark.table(bronze_table)
        .agg(F.max(F.col(effective_column)).alias("max_val"))
        .collect()
    )

    new_watermark = (
        str(new_watermark_row[0]["max_val"])
        if new_watermark_row[0]["max_val"] is not None
        else None
    )

else:

    new_watermark = None

exists = (
    spark.table(CTRL_WATERMARK)
    .filter(
        (F.col("table_name") == table_name) &
        (F.col("schema_name") == schema_name)
    )
    .count() > 0
)

watermark_value = (
    f"'{new_watermark}'"
    if new_watermark is not None
    else "NULL"
)

if exists:

    spark.sql(f"""
        UPDATE {CTRL_WATERMARK}
        SET
            max_incremental_value = {watermark_value},
            last_run_status = 'Succeeded',
            last_run_end_time = current_timestamp(),
            rows_copied = {rows_copied},
            updated_at = current_timestamp()
        WHERE table_name = '{table_name}'
          AND schema_name = '{schema_name}'
    """)

else:

    spark.sql(f"""
        INSERT INTO {CTRL_WATERMARK}
        (
            schema_name,
            table_name,
            max_incremental_value,
            last_run_status,
            last_run_end_time,
            rows_copied,
            created_at,
            updated_at
        )
        VALUES
        (
            '{schema_name}',
            '{table_name}',
            {watermark_value},
            'Succeeded',
            current_timestamp(),
            {rows_copied},
            current_timestamp(),
            current_timestamp()
        )
    """)

run_status = "Succeeded" if new_watermark is not None else "Succeeded_NoRows"

spark.sql(f"""
    INSERT INTO {CTRL_RUN_LOG}
    (pipeline_name, schema_name, table_name, destination_table_name, load_type,
     run_status, rows_read, rows_written, error_message, start_time, end_time, duration_seconds)
    VALUES (
        '{pipeline_name}', '{schema_name}', '{table_name}', '{destination_table}', '{load_type}',
        '{run_status}', {rows_copied}, {rows_copied}, NULL,
        '{run_start.isoformat()}', '{run_end.isoformat()}', {duration_seconds}
    )
""")

print(f"Watermark updated for [{schema_name}].[{table_name}] → {new_watermark} ({rows_copied} rows)")
print(f"Run log entry written: {run_status}, {duration_seconds}s")

mssparkutils.notebook.exit("success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
