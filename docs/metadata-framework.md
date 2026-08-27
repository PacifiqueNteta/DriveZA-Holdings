# Metadata Framework

The platform uses metadata tables to keep ingestion and transformation behavior explicit:

- `pipeline_watermark` stores incremental extraction state.
- `pipeline_control` stores processing control information.
- `pipeline_run_log` records table-level execution outcomes.
- `silver_config` stores Silver business-key and table configuration.
- `schema_change_log` records detected source schema changes.

This approach allows common processing patterns to be reused while keeping source-specific keys and operational state configurable.
