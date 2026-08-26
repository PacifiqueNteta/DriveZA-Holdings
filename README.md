# DriveZA Holdings - Data Platform

## Executive Summary

DriveZA Holdings is a fictional South African vehicle rental company operating across nine provinces, more than 25 cities, and 32 branches with a fleet of approximately 400 vehicles. This project addresses and simulate a common enterprise data problem encountered in some of the projects done for some clients: operational information is distributed across CRM, fleet, and administrative systems, making trusted reporting slow and difficult to govern.

The solution consolidates those sources on Microsoft Fabric using Data Factory pipelines, PySpark notebooks, OneLake Delta Lake storage, a Fabric Warehouse, and a dimensional serving model. The outcome is a traceable Bronze-to-Silver-to-Gold platform that supports consistent reporting, governed self-service analytics, and future AI and machine learning use cases.

## Situation

DriveZA Holdings operates a rental management platform (Bluebird Auto Rental Systems) for customer bookings, payments, and promotions, hosted on-premises on SQL Server . The fleet operations team separately contracted MiX by Powerfleet, fleet telematics provider for vehicle tracking, incident logging, and maintenance scheduling, with telematics data landing in an Azure SQL Server instance the fleet team manages. I used Snowflake here to simulate the Azure SQL Server in this project as I wanted to demonstrate handling a mirrored/shared external database as a source pattern. HR records are maintained in PaySpace, DriveZA's payroll and HR system of record; staff data is periodically exported and saved as a spreadsheet. Branch reference data (manager assignments, fleet capacity) is maintained separately by Branch Operations as its own manually-updated spreadsheet (Used GitHub-hosted files to simulate both). The systems used different schemas, update patterns, and delivery mechanisms. As a result, analysts would need to reconcile customer, rental, payment, vehicle, maintenance, branch, and employee data before producing reliable business views.

## Task

Design and implement an end-to-end data platform that integrates the disconnected sources into a governed medallion architecture. The solution needed to support reliable ingestion, reusable transformation patterns, data quality monitoring, incremental processing, and a business-ready model for reporting and natural-language analysis.

The design also needed to remain extensible: new sources and schema changes should be manageable through configuration and metadata rather than repeated bespoke pipeline development.

## Action

### Platform Architecture

The platform uses Microsoft Fabric as the integration, engineering, storage, and serving environment. Fabric Data Factory orchestrates the workloads, Fabric Notebooks perform PySpark transformations, OneLake stores Delta tables, and `WH_DRZ_GOLD` provides a relational presentation layer.

The architecture separates responsibilities across three medallion layers:

1. **Bronze:** raw, traceable source landing in `LH_DRZ_BRONZE` and the mirrored database `DRIVEZA_FLEET`.
2. **Silver:** standardized, cleansed, deduplicated Delta tables in `LH_DRZ_SILVER`.
3. **Gold:** dimensional, analytics-ready tables in `WH_DRZ_GOLD`.

### Data Sources

Four source feeds are integrated into the platform:

- **CRM:** on-premises SQL Server tables for customers, payments, rentals, promotions, and reviews, connected through a Self-hosted Integration Runtime (SHIR).
- **Fleet management:** Snowflake tables for vehicles, maintenance, and incidents, integrated through the native connection, simulating the Azure SQL Server.
- **Branch administration:** GitHub-hosted CSV data loaded through an HTTP connector simulating the manually maintained Branch Operations data in spreadsheets. 
- **Staff administration:** GitHub-hosted CSV data loaded through an HTTP connector simulating the HR records in PaySpace, the company's payroll and HR system of record accessible to the rest of the business as periodic manual exports saved to a SharePoint spreadsheet.

The design supports both incremental operational sources and full file-based loads, with source-specific control logic rather than forcing every system into the same ingestion pattern.

### Bronze Layer

`LH_DRZ_BRONZE` is the raw landing and observability layer. Source data is captured with minimal transformation so that the original structure remains available for replay, reconciliation, and lineage.

- Fabric pipelines load CRM, branch, and staff data.
- Watermark notebooks manage incremental CRM processing using source `updated_at` values.
- Metadata columns record ingestion timestamps, source system, source file, and load context.
- Schema evolution checks compare incoming CRM structures with existing Bronze tables and record changes.
- Data quality notebooks validate expected tables, row counts, and null patterns before downstream processing.
- Pipeline control and run-log tables provide operational traceability, including failure records.

Fleet data is made available in the workspace by mirroring the Snowflake `DRIVEZA_FLEET` data base in fabric.

### Silver Layer

`LH_DRZ_SILVER` is the curated Delta Lake layer. The Silver transformation reads Bronze tables and the `DRIVEZA_FLEET` mirrored database, then produces consistent tables for downstream modeling.

- Column names and data types are standardized across sources.
- Null tokens and inconsistent values are normalized.
- Duplicate records are removed using configured business keys and ordering rules.
- Record hashes support change detection and efficient incremental merges.
- Delta `MERGE` and overwrite patterns handle inserts, updates, and first-load scenarios.
- Metadata tables such as `metadata.silver_config`, `metadata.pipeline_control`, and `metadata.pipeline_run_log` make processing rules and execution history explicit.
- Pipe-delimited add-on data is expanded into child records where required.

### Gold Layer

`WH_DRZ_GOLD` is the analytics and reporting layer. Silver tables are loaded into a star schema through warehouse stored procedures, separating reusable dimensions from measurable business events.

The model includes date, customer, branch, vehicle, employee, and promotion dimensions, together with rental, payment, maintenance, and incident facts. Slowly changing customer dimension behavior is supported through current-record tracking and effective and expiry dates. This model gives analysts consistent join paths and reusable definitions for operational and commercial KPIs.

### Governance and Security

The platform is designed to support Microsoft Purview as the governance plane: catalog discovery, automated lineage across Fabric assets, data classification, and sensitivity labels can be applied to the source-to-report path. Access should be managed through workspace and item permissions, with least-privilege access to raw, curated, and presentation layers.

The implemented data platform already contributes the operational controls needed for governance, including source metadata, pipeline run history, schema-change logs, and repeatable layer boundaries. Purview registration and policy configuration are the natural extension for enterprise cataloging and compliance; they are not represented as separate configuration files in this repository.

### Business Consumption

The Gold warehouse is intended to serve a Power BI semantic model and reporting layer, with Direct Lake as the serving pattern identified in the project technology stack. The dimensional model provides a stable foundation for executive dashboards, operational reporting, and self-service analysis.

For conversational consumption, the governed semantic model can be connected to a Fabric Data Agent and exposed through Microsoft 365 Copilot. This allows users to ask questions in natural language while keeping answers grounded in approved business entities and measures. The repository contains the data platform and serving foundations; tenant-level Data Agent, Copilot, and report configuration are managed in the Fabric and Microsoft 365 services rather than stored here.

### Architecture Diagram

![DriveZA data platform architecture](architecture/DriveZa%20Architecture.drawio.svg)

## Result

The project produces a single governed path from disconnected operational systems to business-ready analytics. DriveZA can reconcile rental activity with customers, payments, vehicles, branches, employees, maintenance, and incidents without rebuilding source-specific logic in every report.

The metadata-driven controls and medallion boundaries improve operational reliability and make the platform easier to extend as new tables or schema changes appear. Delta Lake storage supports scalable incremental processing, while the Warehouse star schema gives reporting users consistent dimensions, facts, and KPI relationships.

Most importantly, the solution moves analytics closer to self-service without sacrificing traceability. With Purview governance, a certified semantic model, and a Data Agent or Microsoft 365 Copilot consumption path, the platform is positioned for governed natural-language analytics and future predictive or machine learning workloads.

## Technical Implementation

### Source Systems

| Source system | Platform | Data domains | Integration method |
|---|---|---|---|
| CRM | On-premises SQL Server | Customers, payments, rentals, promotions, reviews | Self-hosted Integration Runtime (SHIR) |
| Fleet management | Snowflake | Vehicles, maintenance, incidents | Native Fabric connection |
| Branch administration | GitHub CSV | Branches | HTTP connector |
| Staff administration | GitHub CSV | Staff | HTTP connector |

### Medallion Layers

| Layer | Fabric item | Responsibility |
|---|---|---|
| Bronze | `LH_DRZ_BRONZE` | Raw source landing, ingestion metadata, watermarks, and quality checks |
| Silver | `LH_DRZ_SILVER` | Cleansed, typed, standardized, deduplicated, and incrementally merged Delta tables |
| Gold | `WH_DRZ_GOLD` | Dimensional warehouse model for semantic models and reporting |

### Technology Stack

- **Cloud platform:** Microsoft Fabric
- **Orchestration:** Fabric Data Factory pipelines
- **Transformation:** PySpark Fabric Notebooks and SQL scripts
- **Storage:** OneLake with Delta Lake tables
- **Source systems:** SQL Server 2022, Snowflake, and GitHub raw files
- **Serving:** Fabric Warehouse with T-SQL and Direct Lake semantic model pattern
- **Governance direction:** Microsoft Purview catalog, lineage, classification, and sensitivity labels
- **Version control:** GitHub with Fabric Git integration

### Key Engineering Patterns

- **Incremental watermark loading:** CRM and Snowflake-oriented processing uses source `updated_at` watermarks to limit extraction to changed records and maintain repeatable pipeline state.
- **Metadata-driven pipelines:** Control tables such as `pipeline_watermark`, `pipeline_control`, `pipeline_run_log`, and `silver_config` centralize execution state, business keys, active flags, and transformation behavior.
- **Schema evolution:** Incoming structures are compared with existing Bronze schemas, with compatible additions recorded and handled before transformation continues.
- **Delta Lake patterns:** Silver tables use durable Delta storage for transactional writes, schema management, change detection, and reliable replayable processing.
- **Merge and upsert logic:** Business keys and record hashes drive insert, update, and no-change decisions, avoiding unnecessary rewrites while preserving current data.
- **Data quality checks:** Dedicated validation notebooks inspect expected objects, row counts, null patterns, and load outcomes; failures are captured in pipeline metadata.
- **Dimensional modeling:** Gold uses conformed dimensions and fact tables, including effective and expiry dates for changing customer attributes, to provide predictable reporting relationships.

### Repository Structure

```
DriveZA-Holdings/
├── architecture/
│   ├── Architecture Diagram.png
│   ├── DriveZa Architecture.drawio.png
│   └── DriveZa Architecture.drawio.svg
├── data/
│   ├── README.md
│   └── raw-landing/admin/
│       ├── crm_branches.csv
│       └── hr_staff.csv
├── docs/
├── Fabric/
│   ├── Bronze/
│   │   ├── Notebooks/
│   │   ├── Pipelines/
│   │   └── Strorage/
│   ├── Silver/
│   │   ├── Notebooks/
│   │   ├── Pipelines/
│   │   └── Storage/
│   ├── Gold/
│   │   ├── Notebooks/
│   │   ├── Pipelines/
│   │   └── Storage/
│   └── Readme.md
├── README.md
└── src/
    ├── snowflake/
    │   ├── ddl/
    │   └── loaders/
    └── sql_server/
        ├── ddl/
        └── loaders/
```

## Key Achievements

- Delivered an end-to-end Microsoft Fabric medallion implementation.
- Built a metadata-driven ingestion and transformation framework.
- Integrated SQL Server, Snowflake, and GitHub file sources with source-specific loading patterns.
- Implemented complementary Lakehouse and Warehouse architecture for scalable engineering and business consumption.
- Developed a dimensional serving model for semantic models and Power BI reporting.
- Established governance foundations through lineage-ready metadata, audit logs, classification and sensitivity-label design, and Purview integration planning.
- Enabled a path to governed natural-language analytics through a Fabric Data Agent and Microsoft 365 Copilot integration.