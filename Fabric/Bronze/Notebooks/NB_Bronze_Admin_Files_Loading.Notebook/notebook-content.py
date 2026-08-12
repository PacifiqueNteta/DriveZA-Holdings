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

# **NB_Bronze_Admin_Files_Loading**
# 
# 
# This notebook load the files ingested under files (employees and branches) as delta tables.

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# CONSTANTS
PIPELINE_NAME = "PL_Bronze_Admin"
SOURCE_TYPE = "file"

CTRL_TABLE = "metadata.pipeline_control"
RUN_LOG_TABLE = "metadata.pipeline_run_log"

# START AUDIT
pipeline_run_id = str(uuid.uuid4())
run_start = datetime.now()

#PATH DEFINITION

source_path = (
    f"Files/Raw_Landing/"
    f"{source_system}/"
    f"{file_name}"
)

target_table = (
    f"{destination_schema}.{destination_table_name}"
)

print(f"Source Path      : {source_path}")
print(f"Destination Table: {target_table}")
print(f"Load Type : {load_type}")


# FILE READ
delimiter = column_delimiter
if delimiter is None or delimiter == "":
  delimiter = ","


df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("delimiter", delimiter)
         .csv(source_path)
)


# AUDIT COLUMNS
df = (
    df.withColumn(
        "bronze_load_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "bronze_source_system",
        F.lit(source_system)
    )
    .withColumn(
        "bronze_source_file",
        F.lit(file_name)
    )
)

row_count = df.count()


# SCHEMA CREATION
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {destination_schema}"
)


# LOAD TYPE DEFINITION
write_mode = load_type.lower()

if write_mode not in ["overwrite", "append"]:
  raise Exception( f"Unsupported load_type: {load_type}")


# WRITE DELTA TABLES
writer = (
    df.write
      .format("delta")
      .mode(write_mode)
)

if write_mode == "overwrite":
  writer = writer.option("overwriteSchema","true")

writer.saveAsTable(target_table)

run_end = datetime.now()
duration_seconds = int(
(run_end - run_start).total_seconds())
rows_read = row_count
rows_written = row_count

# LOGGING
print(
    f"Loaded {row_count} rows into {target_table}"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Run_log and pipeline_control update

# CELL ********************

#Control
control_exists = (
    spark.table(CTRL_TABLE)
    .filter(
        (F.col("source_system") == source_system)
        &
        (F.col("schema_name") == destination_schema)
        &
        (F.col("table_name") == destination_table_name)
    )
    .count() > 0
)


if control_exists:

    spark.sql(f"""
        UPDATE {CTRL_TABLE}
        SET
            source_type = '{SOURCE_TYPE}',
            last_run_status = 'Succeeded',
            last_run_end_time = current_timestamp(),
            rows_copied = {rows_written},
            updated_at = current_timestamp()
        WHERE
            source_system = '{source_system}'
        AND schema_name = '{destination_schema}'
        AND table_name = '{destination_table_name}'
    """)

else:

    spark.sql(f"""
        INSERT INTO {CTRL_TABLE}
        (
            source_type,
            source_system,
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
            '{SOURCE_TYPE}',
            '{source_system}',
            '{destination_schema}',
            '{destination_table_name}',
            NULL,
            'Succeeded',
            current_timestamp(),
            {rows_written},
            current_timestamp(),
            current_timestamp()
        )
    """)

#Run_Log

run_end = datetime.now()
duration_seconds = int(
(run_end - run_start).total_seconds()
)
spark.sql(f"""
INSERT INTO {RUN_LOG_TABLE}
(
        pipeline_run_id,
        pipeline_name,
        source_type,
        source_system,
        schema_name,
        table_name,
        destination_table_name,
        load_type,
        run_status,
        rows_read,
        rows_written,
        error_message,
        start_time,
        end_time,
        duration_seconds
)
VALUES
(
        '{pipeline_run_id}',
        '{PIPELINE_NAME}',
        '{SOURCE_TYPE}',
        '{source_system}',
        '{destination_schema}',
        '{destination_table_name}',
        '{destination_table_name}',
        '{load_type}',
        'Succeeded',
        {rows_read},
        {rows_written},
        NULL,
        '{run_start.isoformat()}',
        '{run_end.isoformat()}',
        {duration_seconds}
)
""")

# RETURN TO PIPELINE
mssparkutils.notebook.exit(
f"{destination_schema}|"
f"{destination_table_name}|"
f"{rows_written}|"
f"{pipeline_run_id}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
