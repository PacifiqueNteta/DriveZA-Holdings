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

# ###### 2.Table 'pipeline_control' 

# CELL ********************

spark.sql("""
    CREATE TABLE IF NOT EXISTS metadata.pipeline_control (
        source_type             STRING,
        source_system           STRING,
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
        pipeline_run_id             STRING,
        pipeline_name               STRING,
        source_type               STRING,
        source_system               STRING,
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

# 4. Table 'data_quality'

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.data_quality
(
    source_type                STRING,
    source_name                STRING,

    schema_name               STRING,
    table_name                STRING,

    row_count                 BIGINT,
    column_count              INT,

    duplicate_count           BIGINT,
    duplicate_check_result    STRING,

    null_key_count            BIGINT,
    null_check_result         STRING,

    freshness_days            INT,
    freshness_check_result    STRING,

    overall_score             DECIMAL(5,2),
    overall_status            STRING,

    first_run_at              TIMESTAMP,
    updated_at                TIMESTAMP
)
USING DELTA
""")

print("Table 'metadata.data_quality' ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 5.Table 'data_quality_config'

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.data_quality
(
    quality_run_id     STRING,
    source_type        STRING,
    source_name        STRING,
    schema_name        STRING,
    table_name         STRING,
    check_name         STRING,
    check_result       STRING,
    check_value        STRING,
    checked_at         TIMESTAMP
)
USING DELTA
""")

print("metadata.data_quality ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# vw_data_quality_summary

# CELL ********************

spark.sql("""
CREATE OR REPLACE VIEW metadata.vw_data_quality_summary AS

WITH dq AS
(
    SELECT *
    FROM metadata.data_quality
)

SELECT

    quality_run_id,
    source_type,
    source_name,
    schema_name,
    table_name,

    MAX(
        CASE
            WHEN check_name = 'ROW_COUNT'
            THEN CAST(check_value AS BIGINT)
        END
    ) AS row_count,

    MAX(
        CASE
            WHEN check_name = 'COLUMN_COUNT'
            THEN CAST(check_value AS INT)
        END
    ) AS column_count,

    MAX(
        CASE
            WHEN check_name LIKE 'DUPLICATE%'
            THEN check_result
        END
    ) AS duplicate_check_result,

    MAX(
        CASE
            WHEN check_name LIKE 'DUPLICATE%'
            THEN CAST(check_value AS BIGINT)
        END
    ) AS duplicate_count,

    MAX(
        CASE
            WHEN check_name LIKE 'NULL%'
            THEN check_result
        END
    ) AS null_check_result,

    MAX(
        CASE
            WHEN check_name LIKE 'NULL%'
            THEN CAST(check_value AS BIGINT)
        END
    ) AS null_count,

    ROUND(
        (
            SUM(
                CASE
                    WHEN check_result = 'PASS'
                    THEN 1
                    ELSE 0
                END
            ) * 100.0
        )
        /
        COUNT(*)
    ,2) AS overall_score

FROM dq

GROUP BY

    quality_run_id,
    source_type,
    source_name,
    schema_name,
    table_name
""")

print("metadata.vw_data_quality_summary created.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE OR REPLACE MATERIALIZED LAKE VIEW metadata.data_quality_summary AS

WITH latest_check AS
(
    SELECT
        source_type,
        source_name,
        schema_name,
        table_name,
        MAX(checked_at) AS latest_run_at
    FROM metadata.data_quality
    GROUP BY
        source_type,
        source_name,
        schema_name,
        table_name
)

SELECT

    d.source_type,
    d.source_name,
    d.schema_name,
    d.table_name,

    MAX(
        CASE
            WHEN d.check_name = 'ROW_COUNT'
            THEN CAST(d.check_value AS BIGINT)
        END
    ) AS row_count,

    MAX(
        CASE
            WHEN d.check_name = 'COLUMN_COUNT'
            THEN CAST(d.check_value AS INT)
        END
    ) AS column_count,

    MAX(
        CASE
            WHEN d.check_name LIKE 'DUPLICATE%'
            THEN d.check_result
        END
    ) AS duplicate_check_result,

    MAX(
        CASE
            WHEN d.check_name LIKE 'DUPLICATE%'
            THEN CAST(d.check_value AS BIGINT)
        END
    ) AS duplicate_count,

    MAX(
        CASE
            WHEN d.check_name LIKE 'NULL%'
            THEN d.check_result
        END
    ) AS null_check_result,

    MAX(
        CASE
            WHEN d.check_name LIKE 'NULL%'
            THEN CAST(d.check_value AS BIGINT)
        END
    ) AS null_count,

    ROUND(
        (
            SUM(
                CASE
                    WHEN d.check_result = 'PASS'
                    THEN 1
                    ELSE 0
                END
            ) * 100.0
        ) / COUNT(*),
        2
    ) AS overall_score,

    CASE
        WHEN ROUND(
            (
                SUM(
                    CASE
                        WHEN d.check_result = 'PASS'
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) = 100
        THEN 'Excellent'

        WHEN ROUND(
            (
                SUM(
                    CASE
                        WHEN d.check_result = 'PASS'
                        THEN 1
                        ELSE 0
                    END
                ) * 100.0
            ) / COUNT(*),
            2
        ) >= 75
        THEN 'Good'

        ELSE 'Warning'
    END AS overall_status,

    MAX(d.checked_at) AS latest_run_at

FROM metadata.data_quality d
INNER JOIN latest_check l
    ON d.source_type = l.source_type
   AND d.source_name = l.source_name
   AND d.schema_name = l.schema_name
   AND d.table_name = l.table_name

GROUP BY

    d.source_type,
    d.source_name,
    d.schema_name,
    d.table_name;
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
