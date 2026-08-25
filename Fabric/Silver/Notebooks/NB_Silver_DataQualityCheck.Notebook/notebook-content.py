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

# LH_DRZ_SILVER - all schemas live locally, no mirrored source needed

for schema in ["admn", "crm", "fleet"]:
    for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
        tables.append(
            (
                "lakehouse",
                "LH_DRZ_SILVER",
                schema,
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

# PER-TABLE CHECKS (same shape as Bronze: row count, column
# count, duplicate/null on the table's key column)

dataframes = {}  # cache loaded DataFrames for the referential checks below

for (
    source_type,
    source_name,
    schema_name,
    table_name
) in tables:

    try:
        full_table = f"{schema_name}.{table_name}"
        print(f"Checking {full_table}")

        df = spark.table(full_table)
        dataframes[full_table] = df

        # ROW COUNT
        row_count = df.count()
        add_result(
            source_type, source_name, schema_name, table_name,
            "ROW_COUNT", "PASS" if row_count > 0 else "FAIL", row_count
        )

        # COLUMN COUNT
        add_result(
            source_type, source_name, schema_name, table_name,
            "COLUMN_COUNT", "PASS", len(df.columns)
        )

        # FIRST COLUMN CHECKS (key_column);
        # Every Silver table's first column is its natural ID, per the established schema convention
        if len(df.columns) > 0:
            key_column = df.columns[0]

            duplicate_count = (
                df.groupBy(key_column)
                  .count()
                  .filter(F.col("count") > 1)
                  .count()
            )
            add_result(
                source_type, source_name, schema_name, table_name,
                f"DUPLICATE_{key_column}", "PASS" if duplicate_count == 0 else "FAIL",
                duplicate_count
            )

            null_count = df.filter(F.col(key_column).isNull()).count()
            add_result(
                source_type, source_name, schema_name, table_name,
                f"NULL_{key_column}", "PASS" if null_count == 0 else "FAIL",
                null_count
            )

    except Exception as e:
        add_result(
            source_type, source_name, schema_name, table_name,
            "TABLE_ACCESS", "FAIL", str(e)[:1000]
        )

# REFERENTIAL INTEGRITY CHECKS
# Each entry: (child_table, child_fk_column, parent_table, parent_key_column)

referential_checks = [
    ("crm.rentals", "customer_id", "crm.customers", "customer_id"),
    ("crm.rentals", "vehicle_id", "fleet.vehicles", "vehicle_id"),
    ("crm.payments", "rental_id", "crm.rentals", "rental_id"),
    ("crm.payments", "customer_id", "crm.customers", "customer_id"),
    ("fleet.incidents", "vehicle_id", "fleet.vehicles", "vehicle_id"),
    ("fleet.incidents", "rental_id", "crm.rentals", "rental_id"),
    ("fleet.maintenance", "vehicle_id", "fleet.vehicles", "vehicle_id"),
    ("crm.reviews", "rental_id", "crm.rentals", "rental_id"),
]

for child_table, fk_col, parent_table, pk_col in referential_checks:
    check_name = f"FK_{child_table}.{fk_col}_TO_{parent_table}"
    try:
        if child_table not in dataframes or parent_table not in dataframes:
            add_result(
                "lakehouse", "LH_DRZ_SILVER", child_table.split(".")[0], child_table.split(".")[1],
                check_name, "FAIL", "one or both tables not found in this run"
            )
            continue

        child_df = dataframes[child_table]
        parent_df = dataframes[parent_table]

        if fk_col not in child_df.columns or pk_col not in parent_df.columns:
            add_result(
                "lakehouse", "LH_DRZ_SILVER", child_table.split(".")[0], child_table.split(".")[1],
                check_name, "FAIL", "column not found"
            )
            continue

        orphan_count = (
            child_df.select(fk_col)
                    .filter(F.col(fk_col).isNotNull())
                    .distinct()
                    .join(
                        parent_df.select(pk_col).distinct(),
                        child_df[fk_col] == parent_df[pk_col],
                        "left_anti"
                    )
                    .count()
        )

        schema_name, table_name = child_table.split(".")
        add_result(
            "lakehouse", "LH_DRZ_SILVER", schema_name, table_name,
            check_name, "PASS" if orphan_count == 0 else "FAIL", orphan_count
        )

        if orphan_count > 0:
            print(f"   {check_name}: {orphan_count} orphaned value(s)")

    except Exception as e:
        schema_name, table_name = child_table.split(".")
        add_result(
            "lakehouse", "LH_DRZ_SILVER", schema_name, table_name,
            check_name, "FAIL", str(e)[:1000]
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

print(f"Inserted {result_df.count()} quality check records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
