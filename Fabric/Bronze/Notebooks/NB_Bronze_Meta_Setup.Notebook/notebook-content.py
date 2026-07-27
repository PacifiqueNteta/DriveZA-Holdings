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

# ## **NB_Bronze_Meta_Setup**
# 
# This notebook is a once-off notebook used to setup(create) the `metadata` schema as well as all control tables (`'pipeline_watermark'` and `'pipeline_runlog'`)

# MARKDOWN ********************

# ###### 1. Schema creation

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")
print("Schema 'metadata' ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 2.Table 'pipeline_watermark' 

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS metadata.pipeline_watermark (
        schema_name             STRING NOT NULL,
        table_name               STRING NOT NULL,
        max_incremental_value      STRING,
        last_run_status              STRING,
        last_run_end_time              TIMESTAMP,
        rows_copied                      BIGINT,
        created_at                        TIMESTAMP,
        updated_at                          TIMESTAMP
    )
    USING DELTA
""")
print("Table 'metadata.pipeline_watermark' ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 3. Table 'pipeline_run_log'

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS metadata.pipeline_run_log (
        log_id                      STRING,
        pipeline_name               STRING,
        schema_name                 STRING,
        table_name                  STRING,
        destination_table_name      STRING,
        load_type                   STRING,
        run_status                  STRING,
        rows_read                   BIGINT,
        rows_written                BIGINT,
        error_message               STRING,
        start_time                  TIMESTAMP,
        end_time                    TIMESTAMP,
        duration_seconds            INT
    )
    USING DELTA
""")
print("Table 'metadata.pipeline_run_log' ready.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 4. Table 'schema_change_log'

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS metadata.schema_change_log (
        change_id                STRING,
        schema_name              STRING,
        source_table_name        STRING,
        destination_table_name   STRING,
        change_type              STRING,   
        column_name              STRING,
        old_type                 STRING,
        new_type                 STRING,
        detected_at              TIMESTAMP,
        applied_automatically    BOOLEAN,
        requires_manual_review   BOOLEAN
    )
    USING DELTA
""")

print("Table 'metadata.schema_change_log' ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 5. Verification

# CELL ********************

print("\ Verifying tables")

tables = spark.sql("SHOW TABLES IN metadata").collect()
print(f"Tables in 'metadata' schema: {[t['tableName'] for t in tables]}")

for t in ["pipeline_watermark", "pipeline_run_log", "schema_change_log"]:
    count = spark.table(f"metadata.{t}").count()
    print(f"  metadata.{t}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
