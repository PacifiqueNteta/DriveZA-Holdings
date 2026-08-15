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

# METADATA SCHEMA

spark.sql(""" CREATE SCHEMA IF NOT EXISTS metadata """)


# CONTROL TABLE: one row per source table, current state
spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.pipeline_control (
    source_type           STRING,
    source_name            STRING,
    schema_name            STRING,
    table_name             STRING,
    last_run_status        STRING,
    last_run_end_time      TIMESTAMP,
    rows_read               BIGINT,
    rows_written             BIGINT,
    last_watermark_value   TIMESTAMP,
    created_at               TIMESTAMP,
    updated_at               TIMESTAMP
)
USING DELTA
""")

# PIPELINE RUN LOG

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.pipeline_run_log
(
    pipeline_run_id    STRING,
    pipeline_name      STRING,
    source_type        STRING,
    source_name        STRING,
    schema_name        STRING,
    table_name         STRING,
    run_status         STRING,
    rows_read          BIGINT,
    rows_written       BIGINT,
    inserted_rows      BIGINT,
    updated_rows       BIGINT,
    error_message      STRING,
    run_start          TIMESTAMP,
    run_end            TIMESTAMP,
    duration_seconds   BIGINT
)
USING DELTA
""")

# CONFIG TABLE

spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.silver_config (
    table_name         STRING,    
    schema_name        STRING,
    business_key       STRING,
    watermark_column   STRING,
    is_active          BOOLEAN
)
USING DELTA
""")

print("Silver metadata objects created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
INSERT INTO metadata.silver_config
(
    table_name,
    schema_name,
    business_key,
    watermark_column,
    is_active
)
VALUES

('payments',    'crm',   'payment_id',     'created_at',  true),
('rentals',     'crm',   'rental_id',      'updated_at',  true),
('reviews',     'crm',   'review_id',      'created_at',  true),
('promotions',  'crm',   'promotion_id',   'created_at',  true),

('employees',   'admn',  'employee_id',    'created_at',  true),
('branches',    'admn',  'branch_id',      'updated_at',  true),

('VEHICLES',    'FLEET', 'VEHICLE_ID',     'UPDATED_AT',  true),
('INCIDENTS',   'FLEET', 'INCIDENT_ID',    'UPDATED_AT',  true),
('MAINTENANCE', 'FLEET', 'MAINTENANCE_ID', 'UPDATED_AT',  true),
('BRANCHES',    'FLEET', 'BRANCH_ID',      'UPDATED_AT',  false)
""")

print("Silver Config populated.")
spark.sql("SELECT * FROM metadata.silver_config ORDER BY schema_name, table_name").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



spark.sql("""
CREATE TABLE IF NOT EXISTS metadata.value_normalization_map (
    schema_name          STRING,
    table_name           STRING,
    column_name          STRING,
    raw_value            STRING,
    standardized_value   STRING
)
""")


country_fixes = [
    ("French",    "France"),
    ("Germa",     "Germany"),
    ("Zimbabwea", "Zimbabwe"),
    ("Botswaa",   "Botswana"),
    ("British",   "United Kingdom"),
    ("Mozambica", "Mozambique"),
    ("America",   "United States"),
]

normalization_seed = [
    ("crm", "customers", col, raw, standardized)
    for col in ("country", "license_country")
    for (raw, standardized) in country_fixes
]

seed_df = spark.createDataFrame(
    normalization_seed,
    schema="""
        schema_name STRING,
        table_name STRING,
        column_name STRING,
        raw_value STRING,
        standardized_value STRING
    """
)

seed_df.createOrReplaceTempView("value_normalization_seed")

spark.sql("""
MERGE INTO metadata.value_normalization_map t
USING value_normalization_seed s
ON  t.schema_name = s.schema_name
AND t.table_name  = s.table_name
AND t.column_name = s.column_name
AND t.raw_value    = s.raw_value
WHEN MATCHED THEN UPDATE SET
    t.standardized_value = s.standardized_value
WHEN NOT MATCHED THEN INSERT (schema_name, table_name, column_name, raw_value, standardized_value)
    VALUES (s.schema_name, s.table_name, s.column_name, s.raw_value, s.standardized_value)
""")

print("Value normalization map seeded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE LH_DRZ_SILVER.metadata.pipeline_control
# MAGIC SET last_watermark_value = NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
