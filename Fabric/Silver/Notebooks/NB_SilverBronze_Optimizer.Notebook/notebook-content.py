# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "70a907df-a45a-47a4-9ec5-6eba65f41afb",
# META       "default_lakehouse_name": "LH_DRZ_SILVER",
# META       "default_lakehouse_workspace_id": "a93dcff1-e562-4cf4-9954-8e1176cfc71c",
# META       "known_lakehouses": [
# META         {
# META           "id": "70a907df-a45a-47a4-9ec5-6eba65f41afb"
# META         },
# META         {
# META           "id": "7ef943d7-63c2-467c-bf59-4f02a6e675f3"
# META         }
# META       ]
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": [
# META         {
# META           "id": "2736f7f1-e8b0-4b26-ab8f-8a29a6e6957f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    TimestampType
)
from datetime import datetime
import uuid


WORKSPACE_NAME = "WS-DRZ-MRK-DEV"

BRONZE_LAKEHOUSE = "LH_DRZ_BRONZE"
SILVER_LAKEHOUSE = "LH_DRZ_SILVER"
MIRRORED_DATABASE = "DRIVEZA_FLEET"

# Results are always written to Silver
MAINT_TABLE = f"{SILVER_LAKEHOUSE}.metadata.table_maintenance"

# Delta VACUUM safety: do not go below Delta's default retention (168h / 7 days)
# unless a table has been explicitly reviewed for concurrent long-running readers.
DEFAULT_RETENTION_HOURS = 168

# Per-table retention overrides, keyed by (source_name, schema_name, table_name).
# Only add a table here after confirming no long-running reads or time-travel
# dependency on that table.
RETENTION_OVERRIDES_HOURS = {
    # (SILVER_LAKEHOUSE, "crm", "reviews"): 72,
}

# Per-table ZORDER columns, keyed by (source_name, schema_name, table_name).
# Only columns actually used in filters/joins. Keep to 1-3 columns.
ZORDER_CONFIG = {
    # (SILVER_LAKEHOUSE, "crm", "rentals"): ["customer_id"],
    # (SILVER_LAKEHOUSE, "fleet", "maintenance"): ["vehicle_id", "service_date"],
}

# Tables below this file count aren't worth compacting/vacuuming.
MIN_FILE_COUNT_FOR_MAINTENANCE = 10

maintenance_run_id = str(uuid.uuid4())

tables = []

# LH_DRZ_BRONZE - CRM

for row in spark.sql(f"SHOW TABLES IN {BRONZE_LAKEHOUSE}.crm").collect():

    tables.append(
        (
            "lakehouse",
            BRONZE_LAKEHOUSE,
            "crm",
            row.tableName
        )
    )

# LH_DRZ_BRONZE - ADMIN

for row in spark.sql(f"SHOW TABLES IN {BRONZE_LAKEHOUSE}.admn").collect():

    tables.append(
        (
            "lakehouse",
            BRONZE_LAKEHOUSE,
            "admn",
            row.tableName
        )
    )

# LH_DRZ_BRONZE - DRIVEZA_FLEET (mirrored database)
# Mirrored tables are managed by Fabric's mirroring engine and do not support
# OPTIMIZE / VACUUM - they are discovered here so they are visible in the
# maintenance log, but they are skipped rather than processed below.

fleet_tables = spark.sql(f"""
    SHOW TABLES IN `{WORKSPACE_NAME}`.{MIRRORED_DATABASE}.FLEET
""").collect()

for row in fleet_tables:

    tables.append(
        (
            "mirrored",
            MIRRORED_DATABASE,
            "FLEET",
            row.tableName
        )
    )

# LH_DRZ_SILVER - DBO

for row in spark.sql(f"SHOW TABLES IN {SILVER_LAKEHOUSE}.dbo").collect():

    tables.append(
        (
            "lakehouse",
            SILVER_LAKEHOUSE,
            "dbo",
            row.tableName
        )
    )

# LH_DRZ_SILVER - ADMIN

for row in spark.sql(f"SHOW TABLES IN {SILVER_LAKEHOUSE}.admn").collect():

    tables.append(
        (
            "lakehouse",
            SILVER_LAKEHOUSE,
            "admn",
            row.tableName
        )
    )

# LH_DRZ_SILVER - CRM

for row in spark.sql(f"SHOW TABLES IN {SILVER_LAKEHOUSE}.crm").collect():

    tables.append(
        (
            "lakehouse",
            SILVER_LAKEHOUSE,
            "crm",
            row.tableName
        )
    )

# LH_DRZ_SILVER - FLEET

for row in spark.sql(f"SHOW TABLES IN {SILVER_LAKEHOUSE}.fleet").collect():

    tables.append(
        (
            "lakehouse",
            SILVER_LAKEHOUSE,
            "fleet",
            row.tableName
        )
    )

print(f"Tables discovered: {len(tables)}")

# HELPER

results = []

def add_result(
    source_type,
    source_name,
    schema_name,
    table_name,
    action,
    action_result,
    detail,
    files_before,
    files_after,
    size_before_bytes,
    size_after_bytes,
    duration_seconds
):

    results.append(
        (
            maintenance_run_id,
            source_type,
            source_name,
            schema_name,
            table_name,
            action,
            action_result,
            str(detail)[:1000],
            files_before,
            files_after,
            size_before_bytes,
            size_after_bytes,
            duration_seconds,
            datetime.now()
        )
    )


def get_table_detail(full_table):

    detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]

    return detail["numFiles"], detail["sizeInBytes"]


# MAINTENANCE

for (
    source_type,
    source_name,
    schema_name,
    table_name
) in tables:

    if source_type == "mirrored":

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "OPTIMIZE_VACUUM",
            "SKIPPED",
            "Mirrored database table - maintenance managed by Fabric mirroring",
            None,
            None,
            None,
            None,
            0
        )

        continue

    full_table = f"{source_name}.{schema_name}.{table_name}"

    try:

        print(f"Evaluating {full_table}")

        files_before, size_before = get_table_detail(full_table)

        if files_before < MIN_FILE_COUNT_FOR_MAINTENANCE:

            add_result(
                source_type,
                source_name,
                schema_name,
                table_name,
                "OPTIMIZE_VACUUM",
                "SKIPPED",
                f"Below file threshold ({files_before} files)",
                files_before,
                files_before,
                size_before,
                size_before,
                0
            )

            continue

        # OPTIMIZE (with ZORDER if configured for this table)

        optimize_start = datetime.now()

        zorder_cols = ZORDER_CONFIG.get(
            (source_name, schema_name, table_name)
        )

        if zorder_cols:

            zorder_clause = ", ".join(zorder_cols)

            spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({zorder_clause})")

        else:

            spark.sql(f"OPTIMIZE {full_table}")

        optimize_duration = int(
            (datetime.now() - optimize_start).total_seconds()
        )

        files_after_optimize, size_after_optimize = get_table_detail(full_table)

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "OPTIMIZE",
            "PASS",
            f"ZORDER: {zorder_cols}" if zorder_cols else "No ZORDER",
            files_before,
            files_after_optimize,
            size_before,
            size_after_optimize,
            optimize_duration
        )

        print(
            f"  OPTIMIZE {full_table}: "
            f"{files_before} -> {files_after_optimize} files"
        )

        # VACUUM

        retention_hours = RETENTION_OVERRIDES_HOURS.get(
            (source_name, schema_name, table_name), DEFAULT_RETENTION_HOURS
        )

        vacuum_start = datetime.now()

        spark.sql(f"VACUUM {full_table} RETAIN {retention_hours} HOURS")

        vacuum_duration = int(
            (datetime.now() - vacuum_start).total_seconds()
        )

        files_after_vacuum, size_after_vacuum = get_table_detail(full_table)

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "VACUUM",
            "PASS",
            f"Retention: {retention_hours}h",
            files_after_optimize,
            files_after_vacuum,
            size_after_optimize,
            size_after_vacuum,
            vacuum_duration
        )

        print(
            f"  VACUUM {full_table}: "
            f"{size_after_optimize:,} -> {size_after_vacuum:,} bytes "
            f"(retention {retention_hours}h)"
        )

    except Exception as e:

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "OPTIMIZE_VACUUM",
            "FAIL",
            str(e)[:1000],
            None,
            None,
            None,
            None,
            0
        )

        print(f"  FAILED {full_table}: {str(e)[:200]}")

# SAVE RESULTS

maintenance_schema = StructType([
    StructField("maintenance_run_id", StringType()),
    StructField("source_type", StringType()),
    StructField("source_name", StringType()),
    StructField("schema_name", StringType()),
    StructField("table_name", StringType()),
    StructField("action", StringType()),
    StructField("action_result", StringType()),
    StructField("detail", StringType()),
    StructField("files_before", LongType()),
    StructField("files_after", LongType()),
    StructField("size_before_bytes", LongType()),
    StructField("size_after_bytes", LongType()),
    StructField("duration_seconds", IntegerType()),
    StructField("run_at", TimestampType())
])

result_df = spark.createDataFrame(results, schema=maintenance_schema)

(
    result_df.write
        .mode("append")
        .format("delta")
        .saveAsTable(MAINT_TABLE)
)

print(
    f"Inserted {result_df.count()} maintenance records into {MAINT_TABLE}."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
