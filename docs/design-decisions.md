# Design Decisions

- Microsoft Fabric provides a single environment for orchestration, notebooks, OneLake, lakehouse processing, warehouse serving, and semantic-model foundations.
- Bronze, Silver, and Gold separate raw preservation, curation, and business presentation responsibilities.
- Metadata-driven processing keeps watermarks, business keys, and control state reusable across tables.
- A Warehouse star schema provides predictable reporting relationships after flexible lakehouse processing.
- Source-specific ingestion patterns reflect the different change and delivery behavior of SQL Server, Snowflake, and file sources.
