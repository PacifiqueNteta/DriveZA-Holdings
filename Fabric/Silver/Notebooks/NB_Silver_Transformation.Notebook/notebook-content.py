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
# META           "id": "7ef943d7-63c2-467c-bf59-4f02a6e675f3"
# META         },
# META         {
# META           "id": "70a907df-a45a-47a4-9ec5-6eba65f41afb"
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

# MARKDOWN ********************

# # Silver Load: Bronze to Silver Transformation
# 
# Transforms Bronze Lakehouse (`crm`, `admn`) and Mirrored Database (`FLEET`) source tables
# into curated Silver Delta tables: 
# - incremental extraction, 
# - cleansing, 
# - deduplication,
# - change-detection hashing, and 
# - MERGE/overwrite load.
# 
# **Prerequisite:** run `silver_metadata_setup.py` once before the first execution of this
# notebook - it creates `metadata.pipeline_control`, `metadata.pipeline_run_log`, and
# `metadata.silver_config` (business keys + active flags, required per table).

# MARKDOWN ********************

# ## Imports & structured logging setup

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

import uuid
import re
import json
import logging

from datetime import datetime
from functools import reduce


# Structured (JSON) logging, so log lines are queryable instead of free-text print() output.
class JsonFormatter(logging.Formatter):

    def format(self, record):

        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage()
        }

        if hasattr(record, "extra_fields"):

            payload.update(record.extra_fields)

        return json.dumps(payload)


logger = logging.getLogger("silver_pipeline")
logger.setLevel(logging.INFO)
logger.handlers = [logging.StreamHandler()]
logger.handlers[0].setFormatter(JsonFormatter())
logger.propagate = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration
# 
# Pipeline identifiers, metadata table names, workspace/lakehouse references,
# Business Event alerting config, and cleansing constants.

# CELL ********************

PIPELINE_NAME = "PL_Silver_Load"

CTRL_TABLE = "metadata.pipeline_control"
RUN_LOG_TABLE = "metadata.pipeline_run_log"
BUSINESS_KEY_CONFIG_TABLE = "metadata.silver_config"
VALUE_NORMALIZATION_TABLE = "metadata.value_normalization_map"

# Failure alerting via Fabric Business Events (preview). Requires a
# Business Event schema created in advance in Real-Time hub, and an
# Activator rule subscribed to it (Teams/email/run pipeline). Set
# ENABLE_FAILURE_ALERTS = False to disable without removing the hook.
ENABLE_FAILURE_ALERTS = False
BUSINESS_EVENT_WORKSPACE = "<workspace_name_or_id>"
BUSINESS_EVENT_SCHEMA_SET = "<schema_set_name>"
BUSINESS_EVENT_TYPE_NAME = "SilverPipelineTableFailed"

pipeline_run_id = str(uuid.uuid4())
run_start = datetime.now()

WORKSPACE_NAME = "WS-DRZ-MRK-DEV"
BRONZE_LAKEHOUSE = "LH_DRZ_BRONZE"
MIRRORED_DATABASE = "DRIVEZA_FLEET"

NULL_TOKENS = [
    "",
    "NULL",
    "null",
    "Null",
    "N/A",
    "n/a",
    "NA",
    "na",
    "UNKNOWN",
    "Unknown",
    "unknown"
]

# Technical columns dropped at read time
TECHNICAL_COLUMNS_TO_DROP = [
    "metadata_row_id"
]

AUDIT_COLUMNS = [
    "silver_load_timestamp",
    "record_hash",
    "record_created_at",
    "record_updated_at"
]

# Enable MERGE schema evolution
spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper functions

# CELL ********************

def standardize_name(name):

    return (
        re.sub(
            r'[^a-zA-Z0-9]+',
            '_',
            name.strip()
        )
        .lower()
        .strip('_')
    )


def get_source_table_ref(source_type, source_name, schema_name, table_name):

    if source_type == "lakehouse":

        # Four-part namespace required: the default lakehouse
        # (LH_DRZ_SILVER) is schema-enabled, so a bare schema.table
        # reference resolves against it, not LH_DRZ_BRONZE. Both
        # lakehouses are in the same workspace.
        return (
            f"`{WORKSPACE_NAME}`."
            f"{BRONZE_LAKEHOUSE}."
            f"{schema_name}."
            f"{table_name}"
        )

    return (
        f"`{WORKSPACE_NAME}`."
        f"{MIRRORED_DATABASE}."
        f"{schema_name}."
        f"{table_name}"
    )


def publish_failure_alert(
    source_type,
    source_name,
    schema_name,
    table_name,
    error_message
):
    """
    Publish a Business Event on table failure, for Data Activator to react to. Never raises. An alerting failure must not affect the pipeline itself.
    """

    if not ENABLE_FAILURE_ALERTS:

        return

    try:

        notebookutils.businessEvents.publish(
            eventSchemaSetWorkspace=BUSINESS_EVENT_WORKSPACE,
            eventSchemaSet=BUSINESS_EVENT_SCHEMA_SET,
            eventTypeName=BUSINESS_EVENT_TYPE_NAME,
            eventData={
                "pipelineRunId": pipeline_run_id,
                "pipelineName": PIPELINE_NAME,
                "sourceType": source_type,
                "sourceName": source_name,
                "schemaName": schema_name,
                "tableName": table_name,
                "errorMessage": error_message or "",
                "eventTime": datetime.utcnow().isoformat()
            }
        )

    except Exception as alert_error:

        logger.warning(
            "Failed to publish failure business event",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "alert_error": str(alert_error)[:500]
            }}
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `process_table()`
# 
# - Extract (watermark-filtered, four-part namespace) -> 
# - column pruning + rename ->
# - null-token cleansing -> 
# - business-key validation + null filter -> 
# - window-based dedup ->
# - audit columns + `xxhash64` change hash -> 
# - MERGE (schema evolution enabled) or initial overwrite. 
# - Returns a metrics dict
# 
# **PS:** Does not write to the control table or run log.

# CELL ********************

def process_table(
    source_type,
    source_name,
    schema_name,
    table_name,
    last_watermark,
    business_keys,
    configured_watermark_column,
    value_normalizations
):
    """
    Extract, cleanse, deduplicate, hash, and merge/write a single
    source table into its corresponding Silver Delta table.
    Returns a metrics dict; does not write to the control table or
    run log itself.
    """


    source_table = get_source_table_ref(
        source_type, source_name, schema_name, table_name
    )

    raw_df = spark.table(source_table)

    raw_columns_by_lower = {c.lower(): c for c in raw_df.columns}
    raw_watermark_col = raw_columns_by_lower.get(
        configured_watermark_column.lower()
    )

    if raw_watermark_col is None:

        raise ValueError(
            f"Configured watermark column "
            f"'{configured_watermark_column}' not found in "
            f"{schema_name}.{table_name}. Available columns: "
            f"{raw_df.columns}"
        )

    if raw_watermark_col is None:
 
        raise ValueError(
            f"Configured watermark column "
            f"'{configured_watermark_column}' not found in "
            f"{schema_name}.{table_name}. Available columns: "
            f"{raw_df.columns}"
        )
 
    if last_watermark is not None:
 
        logger.info(
            "Incremental load",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "watermark_column": raw_watermark_col,
                "last_watermark": str(last_watermark)
            }}
        )
 
        raw_df = raw_df.filter(
            F.col(raw_watermark_col) > F.lit(last_watermark)
        )
 
    else:
 
        logger.info(
            "Full load - no prior watermark",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name
            }}
        )
 
    # Post-rename name, used for everything after the rename/select
    # below (aggregation, window ordering) - standardize_name() is
    # idempotent, so this matches whatever the rename step produces.
    watermark_col = standardize_name(raw_watermark_col)

    # Column pruning + rename/standardize
    rename_exprs = [
        F.col(field.name).alias(standardize_name(field.name))
        for field in raw_df.schema.fields
        if field.name not in TECHNICAL_COLUMNS_TO_DROP
    ]

    df = raw_df.select(*rename_exprs)

    # Null-token cleansing
    cleanse_exprs = []

    for field in df.schema.fields:

        if field.dataType.simpleString() == "string":

            cleanse_exprs.append(
                F.when(
                    F.trim(F.col(field.name)).isin(NULL_TOKENS),
                    None
                )
                .otherwise(
                    F.trim(F.col(field.name))
                )
                .alias(field.name)
            )

        else:

            cleanse_exprs.append(F.col(field.name))

    df = df.select(*cleanse_exprs)

    # Phone number formatting: any column whose name contains "phone"
    # is cast to string, stripped of non-digit characters, and
    # left-padded to 10 digits if it's currently 1-9 digits. 10+ digit
    # values (e.g. with a country code) pass through unchanged - no
    # country-code normalization here. A fully-empty result after
    # stripping becomes NULL rather than a fabricated value.
    phone_exprs = []
 
    for field in df.schema.fields:
 
        if "phone" in field.name.lower():
 
            digits_only = F.regexp_replace(
                F.col(field.name).cast("string"), r"[^0-9]", ""
            )
 
            phone_exprs.append(
                F.when(
                    (F.length(digits_only) > 0) & (F.length(digits_only) < 10),
                    F.lpad(digits_only, 10, "0")
                )
                .when(F.length(digits_only) == 0, F.lit(None))
                .otherwise(digits_only)
                .alias(field.name)
            )
 
        else:
 
            phone_exprs.append(F.col(field.name))
 
    df = df.select(*phone_exprs)

    # Value normalization - config-driven corrections for known-bad
    # free-text values (e.g. country name typos/truncations). No-op
    # if this table has no rows in metadata.value_normalization_map.
    for column_name, mapping in value_normalizations.items():

        if column_name in df.columns:

            df = df.replace(mapping, subset=[column_name])

    # Target table name, computed early - needed by the empty-batch
    # early exit below as well as the write path further down.
    target_schema = standardize_name(schema_name)
    target_table = standardize_name(table_name)
    target_fqn = f"{target_schema}.{target_table}"


    # Row count + new watermark, single aggregation pass
    agg_row = df.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.max(F.col(watermark_col)).alias("max_watermark")
    ).collect()[0]

    rows_read = agg_row["row_count"]

    new_watermark = (
        agg_row["max_watermark"]
        if agg_row["max_watermark"] is not None
        else last_watermark
    )
    
    # Empty-batch early exit - nothing new since the last successful
    # run, so skip rename-already-done cleanup, dedup, hashing, and
    # the MERGE entirely. Output semantics are identical to running
    # a MERGE against an empty source (0/0/0), just without the
    # wasted compute of getting there.
    if rows_read == 0:

        logger.info(
            "No new records found",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name
            }}
        )

        return {
            "target_fqn": target_fqn,
            "business_key": ",".join(business_keys),
            "rows_read": 0,
            "rows_written": 0,
            "inserted_rows": 0,
            "updated_rows": 0,
            "new_watermark": new_watermark
        }

    # Business-key validation + early null filter
    missing_keys = [k for k in business_keys if k not in df.columns]

    if missing_keys:

        raise ValueError(
            f"Configured business key column(s) {missing_keys} not "
            f"found in {schema_name}.{table_name} after cleansing. "
            f"Available columns: {df.columns}"
        )

    df = df.filter(
        reduce(
            lambda a, b: a & b,
            [F.col(k).isNotNull() for k in business_keys]
        )
    )

    # Window-based dedup on business key(s)
    window_spec = (
        Window
        .partitionBy(*business_keys)
        .orderBy(F.col(watermark_col).desc())
    )

    df = (
        df.withColumn(
            "_rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Audit columns + xxhash64 change-detection hash
    df = df.withColumn(
        "silver_load_timestamp",
        F.current_timestamp()
    )

    hash_columns = [
        c for c in df.columns if c not in AUDIT_COLUMNS
    ]

    df = df.withColumn(
        "record_hash",
        F.xxhash64(*[F.col(c) for c in hash_columns])
    )

    df = (
        df
        .withColumn("record_created_at", F.current_timestamp())
        .withColumn("record_updated_at", F.current_timestamp())
    )

    # MERGE (schema evolution enabled) or initial overwrite
    target_schema = standardize_name(schema_name)
    target_table = standardize_name(table_name)
    target_fqn = f"{target_schema}.{target_table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    if not spark.catalog.tableExists(target_fqn):

        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .saveAsTable(target_fqn)
        )

        inserted_rows = df.count()
        updated_rows = 0
        rows_written = inserted_rows

    else:

        delta_target = DeltaTable.forName(spark, target_fqn)

        merge_condition = " AND ".join(
            f"t.{k} = s.{k}" for k in business_keys
        )

        update_set = {
            c: f"s.{c}"
            for c in df.columns
            if c not in ("record_created_at", "record_updated_at")
        }
        update_set["record_updated_at"] = "current_timestamp()"

        (
            delta_target.alias("t")
            .merge(
                df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition="t.record_hash <> s.record_hash",
                set=update_set
            )
            .whenNotMatchedInsert(
                values={c: f"s.{c}" for c in df.columns}
            )
            .execute()
        )

        # Real merge metrics from the Delta transaction log
        latest_op = spark.sql(f"""
            DESCRIBE HISTORY {target_fqn} LIMIT 1
        """).collect()[0]

        op_metrics = latest_op["operationMetrics"] or {}

        inserted_rows = int(op_metrics.get("numTargetRowsInserted", 0))
        updated_rows = int(op_metrics.get("numTargetRowsUpdated", 0))
        rows_written = inserted_rows + updated_rows

    return {
        "target_fqn": target_fqn,
        "business_key": ",".join(business_keys),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "inserted_rows": inserted_rows,
        "updated_rows": updated_rows,
        "new_watermark": new_watermark
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table discovery + lookups
# 
# Builds the list of source tables to process, the per-table watermark lookup (from the
# control table), the per-table business-key/watermark-column/active-flag lookup (from
# `metadata.silver_config`), and the per-table/column value-normalization lookup (from
# `metadata.value_normalization_map`). All lookups are keyed case-insensitively.

# CELL ********************

# Discover source tables
tables = []

for row in spark.sql(f"""
SHOW TABLES IN `{WORKSPACE_NAME}`.{BRONZE_LAKEHOUSE}.crm
""").collect():

    tables.append(
        ("lakehouse", "LH_DRZ_BRONZE", "crm", row.tableName)
    )

for row in spark.sql(f"""
SHOW TABLES IN `{WORKSPACE_NAME}`.{BRONZE_LAKEHOUSE}.admn
""").collect():

    tables.append(
        ("lakehouse", "LH_DRZ_BRONZE", "admn", row.tableName)
    )

fleet_tables = spark.sql(f"""
SHOW TABLES IN `{WORKSPACE_NAME}`.{MIRRORED_DATABASE}.FLEET
""").collect()

for row in fleet_tables:

    tables.append(
        ("mirrored", MIRRORED_DATABASE, "FLEET", row.tableName)
    )


# Watermark lookup
control_lookup = {}

if spark.catalog.tableExists(CTRL_TABLE):

    control_rows = spark.sql(f"""
        SELECT
            schema_name,
            table_name,
            last_watermark_value
        FROM {CTRL_TABLE}
        WHERE last_run_status = 'Succeeded'
        AND last_watermark_value IS NOT NULL
    """).collect()

    for row in control_rows:

        control_lookup[
            (row.schema_name, row.table_name)
        ] = row.last_watermark_value


# Table config lookup (business key + watermark_column + active flag )
table_config_lookup = {}

if spark.catalog.tableExists(BUSINESS_KEY_CONFIG_TABLE):

    config_rows = spark.sql(f"""
        SELECT
            schema_name,
            table_name,
            business_key,
            watermark_column,
            is_active
        FROM {BUSINESS_KEY_CONFIG_TABLE}
    """).collect()

    for row in config_rows:

        table_config_lookup[
            (
               row.schema_name.strip().lower(),
               row.table_name.strip().lower()
            )
    ] = {
            "business_keys": [
                standardize_name(k.strip())
                for k in (row.business_key or "").split(",")
                if k.strip()
            ],
            "watermark_column": row.watermark_column,
            "is_active": (
                row.is_active if row.is_active is not None else True
            )
        }

# Value normalization lookup (config-driven corrections, per column)
value_normalization_lookup = {}

if spark.catalog.tableExists(VALUE_NORMALIZATION_TABLE):

    norm_rows = spark.sql(f"""
        SELECT
            schema_name,
            table_name,
            column_name,
            raw_value,
            standardized_value
        FROM {VALUE_NORMALIZATION_TABLE}
    """).collect()

    for row in norm_rows:

        key = (row.schema_name.lower(), row.table_name.lower())
        value_normalization_lookup.setdefault(key, {})
        value_normalization_lookup[key].setdefault(row.column_name, {})
        value_normalization_lookup[key][row.column_name][
            row.raw_value
        ] = row.standardized_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main loop
# 
# - Per-table processing with error isolation: missing config -> 
# - Failed & alerted, inactive -> 
# - Skipped, active -> 
# - processed. Control table and run log are written regardless of outcome, with their own isolated try/except.

# CELL ********************

# Main loop
for (
    source_type,
    source_name,
    schema_name,
    table_name
) in tables:

    logger.info(
        "Processing table",
        extra={"extra_fields": {
            "pipeline_run_id": pipeline_run_id,
            "source_type": source_type,
            "source_name": source_name,
            "schema_name": schema_name,
            "table_name": table_name
        }}
    )

    table_start = datetime.now()

    last_watermark = control_lookup.get(
        (schema_name.lower(), table_name.lower())
    )

    table_config = table_config_lookup.get(
        (schema_name.lower(), table_name.lower())
    )

    value_normalizations = value_normalization_lookup.get(
        (schema_name.lower(), table_name.lower()), {}
    )

    run_status = "Succeeded"
    error_message = None
    result = None

    if table_config is None:

        run_status = "Failed"
        error_message = (
            f"No business key configured for "
            f"{schema_name}.{table_name} in "
            f"{BUSINESS_KEY_CONFIG_TABLE}. Add a row before "
            f"this table can be processed."
        )

        logger.error(
            "Table processing failed",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "error": error_message
            }}
        )

        publish_failure_alert(
            source_type,
            source_name,
            schema_name,
            table_name,
            error_message
        )

    elif not table_config["is_active"]:

        run_status = "Skipped"

        logger.info(
            "Table is inactive - skipping",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name
            }}
        )

    elif not table_config["business_keys"]:

        run_status = "Failed"
        error_message = (
            f"{schema_name}.{table_name} is marked active in "
            f"{BUSINESS_KEY_CONFIG_TABLE} but has no business_key "
            f"value set."
        )

        logger.error(
            "Table processing failed",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "error": error_message
            }}
        )

        publish_failure_alert(
            source_type,
            source_name,
            schema_name,
            table_name,
            error_message
        )

    elif not table_config["watermark_column"]:

        run_status = "Failed"
        error_message = (
            f"{schema_name}.{table_name} is marked active in "
            f"{BUSINESS_KEY_CONFIG_TABLE} but has no "
            f"watermark_column value set."
        )

        logger.error(
            "Table processing failed",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "error": error_message
            }}
        )

        publish_failure_alert(
            source_type,
            source_name,
            schema_name,
            table_name,
            error_message
        )

    else:

        try:

            result = process_table(
                source_type,
                source_name,
                schema_name,
                table_name,
                last_watermark,
                table_config["business_keys"],
                table_config["watermark_column"],
                value_normalizations
            )

            logger.info(
                "Rows read",
                extra={"extra_fields": {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "rows_read": result["rows_read"]
                }}
            )

        except Exception as e:

            run_status = "Failed"
            error_message = str(e)[:2000]

            logger.error(
                "Table processing failed",
                extra={"extra_fields": {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "error": error_message
                }}
            )

            publish_failure_alert(
                source_type,
                source_name,
                schema_name,
                table_name,
                error_message
            )

    # Resolve metrics for logging
    if result is not None:

        rows_read = result["rows_read"]
        rows_written = result["rows_written"]
        inserted_rows = result["inserted_rows"]
        updated_rows = result["updated_rows"]
        new_watermark = result["new_watermark"]

    else:

        rows_read = 0
        rows_written = 0
        inserted_rows = 0
        updated_rows = 0
        new_watermark = None

    table_end = datetime.now()

    duration_seconds = int(
        (table_end - table_start).total_seconds()
    )

    # Control table + run log
    try:

        spark.sql("""
        MERGE INTO {ctrl} t
        USING
        (
            SELECT
                :source_type AS source_type,
                :source_name AS source_name,
                :schema_name AS schema_name,
                :table_name AS table_name,
                :run_status AS run_status,
                :rows_read AS rows_read,
                :rows_written AS rows_written,
                :watermark_value AS watermark_value
        ) s
        ON
        t.schema_name = s.schema_name
        AND t.table_name = s.table_name

        WHEN MATCHED THEN UPDATE SET

            last_run_status = s.run_status,
            last_run_end_time = current_timestamp(),
            rows_read = CASE
                WHEN s.run_status = 'Succeeded' THEN s.rows_read
                ELSE t.rows_read
            END,
            rows_written = CASE
                WHEN s.run_status = 'Succeeded' THEN s.rows_written
                ELSE t.rows_written
            END,
            last_watermark_value = CASE
                WHEN s.run_status = 'Succeeded' THEN s.watermark_value
                ELSE t.last_watermark_value
            END,
            updated_at = current_timestamp()

        WHEN NOT MATCHED THEN
        INSERT
        (
            source_type,
            source_name,
            schema_name,
            table_name,
            last_run_status,
            last_run_end_time,
            rows_read,
            rows_written,
            last_watermark_value,
            created_at,
            updated_at
        )
        VALUES
        (
            s.source_type,
            s.source_name,
            s.schema_name,
            s.table_name,
            s.run_status,
            current_timestamp(),
            s.rows_read,
            s.rows_written,
            s.watermark_value,
            current_timestamp(),
            current_timestamp()
        )
        """.format(ctrl=CTRL_TABLE), args={
            "source_type": source_type,
            "source_name": source_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "run_status": run_status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "watermark_value": new_watermark
        })

        spark.sql("""
        INSERT INTO {run_log}
        VALUES
        (
            :pipeline_run_id,
            :pipeline_name,
            :source_type,
            :source_name,
            :schema_name,
            :table_name,
            :run_status,
            :rows_read,
            :rows_written,
            :inserted_rows,
            :updated_rows,
            :error_message,
            :run_start,
            :run_end,
            :duration_seconds
        )
        """.format(run_log=RUN_LOG_TABLE), args={
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": PIPELINE_NAME,
            "source_type": source_type,
            "source_name": source_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "run_status": run_status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "inserted_rows": inserted_rows,
            "updated_rows": updated_rows,
            "error_message": error_message,
            "run_start": table_start,
            "run_end": table_end,
            "duration_seconds": duration_seconds
        })

    except Exception as log_error:

        logger.warning(
            "Failed to write audit log",
            extra={"extra_fields": {
                "schema_name": schema_name,
                "table_name": table_name,
                "log_error": str(log_error)[:500]
            }}
        )

    continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
