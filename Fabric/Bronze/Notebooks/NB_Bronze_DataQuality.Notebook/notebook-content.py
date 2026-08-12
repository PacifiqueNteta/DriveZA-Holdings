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
from datetime import datetime
import uuid

QUALITY_TABLE = "metadata.data_quality"

quality_run_id = str(uuid.uuid4())

tables = []

# LH_DRZ_BRONZE - CRM

for row in spark.sql("SHOW TABLES IN crm").collect():

    tables.append(
        (
            "lakehouse",
            "LH_DRZ_BRONZE",
            "crm",
            row.tableName
        )
    )

# LH_DRZ_BRONZE - ADMIN

for row in spark.sql("SHOW TABLES IN admn").collect():

    tables.append(
        (
            "lakehouse",
            "LH_DRZ_BRONZE",
            "admn",
            row.tableName
        )
    )

# DRIVEZA_FLEET MIRRORED DATABASE

fleet_tables = spark.sql("""
SHOW TABLES IN `WS-DRZ-MRK-DEV`.DRIVEZA_FLEET.FLEET
""").collect()

for row in fleet_tables:

    if row.tableName.upper() != "BRANCHES":

        tables.append(
            (
                "mirrored",
                "DRIVEZA_FLEET",
                "FLEET",
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
    check_name,
    check_result,
    check_value
):

    results.append(
        (
            quality_run_id,
            source_type,
            source_name,
            schema_name,
            table_name,
            check_name,
            check_result,
            str(check_value),
            datetime.now()
        )
    )

# QUALITY CHECKS

for (
    source_type,
    source_name,
    schema_name,
    table_name
) in tables:

    try:

        # BUILD TABLE REFERENCE

        if source_type == "lakehouse":

            full_table = (
                f"{schema_name}.{table_name}"
            )

        else:

            full_table = (
                f"`WS-DRZ-MRK-DEV`."
                f"DRIVEZA_FLEET."
                f"{schema_name}."
                f"{table_name}"
            )

        print(f"Checking {full_table}")

        df = spark.table(full_table)

        # ROW COUNT

        row_count = df.count()

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "ROW_COUNT",
            "PASS" if row_count > 0 else "FAIL",
            row_count
        )

        # COLUMN COUNT

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "COLUMN_COUNT",
            "PASS",
            len(df.columns)
        )

        # FIRST COLUMN CHECKS

        if len(df.columns) > 0:

            key_column = df.columns[0]

            # Duplicate check

            duplicate_count = (
                df.groupBy(key_column)
                  .count()
                  .filter(F.col("count") > 1)
                  .count()
            )

            add_result(
                source_type,
                source_name,
                schema_name,
                table_name,
                f"DUPLICATE_{key_column}",
                "PASS" if duplicate_count == 0 else "FAIL",
                duplicate_count
            )

            # Null check

            null_count = (
                df.filter(F.col(key_column).isNull())
                  .count()
            )

            add_result(
                source_type,
                source_name,
                schema_name,
                table_name,
                f"NULL_{key_column}",
                "PASS" if null_count == 0 else "FAIL",
                null_count
            )

    except Exception as e:

        add_result(
            source_type,
            source_name,
            schema_name,
            table_name,
            "TABLE_ACCESS",
            "FAIL",
            str(e)[:1000]
        )

# SAVE RESULTS

result_df = spark.createDataFrame(
    results,
    [
        "quality_run_id",
        "source_type",
        "source_name",
        "schema_name",
        "table_name",
        "check_name",
        "check_result",
        "check_value",
        "checked_at"
    ]
)

(
    result_df.write
        .mode("append")
        .format("delta")
        .saveAsTable(QUALITY_TABLE)
)

print(
    f"Inserted {result_df.count()} quality check records."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
