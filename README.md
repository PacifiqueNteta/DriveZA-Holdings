# DriveZA Holdings — Data Platform

 End-to-end data engineering solution simulating a project done for a client built on Microsoft Fabric following medallion architecture (Bronze → Silver → Gold).

## Company Overview

DriveZA Holdings (Pty) Ltd is a fictional South African vehicle rental company
operating across 9 provinces, 25+ cities, and 32 branches with a fleet of 400
vehicles. This project builds a production-grade data platform ingesting from
multiple source systems into a governed, versioned lakehouse.

## Architecture

### 1. Main

The platform follows a medallion architecture built on Microsoft Fabric:

- Bronze stores raw source data with minimal processing and full lineage.
- Silver applies cleansing, standardization, deduplication, and incremental merge logic.
- Gold exposes curated business-ready dimensions and facts for reporting and analytics.

![Architecture Diagram](architecture/DriveZa%20Architecture.drawio.svg)

### 2. Bronze Layer

The Bronze layer is implemented in the Fabric lakehouse `LH_DRZ_BRONZE` and is the landing zone for all source systems before any business logic is applied.

- Source ingestion is handled by Fabric pipelines for the CRM, admin, and fleet feeds.
- Raw CSV files from GitHub are loaded into `admin` and `crm` landing areas, while operational data from SQL Server and Snowflake is staged with watermarks and schema checks.
- The layer preserves the original source structure and adds governance metadata such as ingestion timestamps, source names, and load markers.
- Data quality checks validate row counts, null patterns, and expected schema evolution before moving data forward.
- Key Bronze notebooks include:
  - `NB_Bronze_Admin_Files_Loading`
  - `NB_Bronze_Crm_GetWatermark`
  - `NB_Bronze_Crm_UpdateWatermark`
  - `NB_Bronze_Crm_SchemaEvolutionCheck`
  - `NB_Bronze_DataQuality`
  - `NB_Bronze_Meta_Setup`

At this stage, the data is still considered raw and is not meant for analytical consumption. The purpose is to capture exactly what arrived from the source systems and keep a reliable audit trail.

### 3. Silver Layer

The Silver layer is implemented in the Fabric lakehouse `LH_DRZ_SILVER` and acts as the curated business-ready zone.

- Bronze tables are transformed into standardized Delta tables with consistent column naming, type normalization, null handling, and deduplication.
- The Silver notebook `NB_Silver_Transformation` reads from Bronze and the fleet mirrored database, then applies incremental loading and merge logic based on business keys and record hashes.
- Key responsibilities include:
  - standardizing field names and values
  - removing duplicate records
  - handling late-arriving changes and updates
  - detecting record changes using hash-based comparison
  - maintaining pipeline control and run logging metadata
  - creating durable, query-friendly tables for downstream consumption

The Silver architecture also manages metadata configuration through `metadata.silver_config`, which controls business keys, active flags, and table-level transformation rules. This makes the layer reproducible and auditable as new source tables and schema changes are introduced.

### 4. Gold Layer

The Gold layer is implemented in the Fabric warehouse `WH_DRZ_GOLD` and represents the analytics-ready dimensional model for business reporting.

- It transforms Silver data into a star schema optimized for dashboards and KPI analysis.
- Gold tables are built using warehouse stored procedures such as:
  - `uspLoadDimCustomer`
  - `uspLoadDimBranch`
  - `uspLoadDimVehicle`
  - `uspLoadDimEmployees`
  - `uspLoadDimPromotion`
  - `uspLoadDimDate`
  - `uspLoadFactRental`
  - `uspLoadFactPayment`
  - `uspLoadFactMaintenance`
  - `uspLoadFactIncident`
- The gold model includes a mix of dimensions and facts that support customer, fleet, branch, employee, and rental performance reporting across DriveZA’s operations.
- This is the presentation layer used for semantic models, executive dashboards, and operational analytics.

The result is a clean, governed, business-facing data product that customers and analysts can consume without needing to understand raw source details or transformation logic.


### Source Systems

| System | Platform | Tables | Connector |
|---|---|---|---|
| CRM System | SQL Server On-Premise | customers, payments, rentals, promotions, reviews | SHIR |
| Fleet Management | Snowflake | vehicles, maintenance, incidents | Native |
| Admin System | GitHub (CSV) | branches | HTTP |
| Admin System | GitHub (CSV) | staff | HTTP |

### Medallion Layers

| Layer | Fabric Item | Purpose |
|---|---|---|
| Bronze | `LH_DRZ_BRONZE` | Raw landed data — no transformations |
| Silver | `LH_DRZ_SILVER` | Cleaned, typed, deduplicated, incremental |
| Gold | `WH_DRZ_GOLD` | Star schema — Dims and Facts |

## Tech Stack

- **Orchestration:** Microsoft Fabric Data Factory
- **Transformation:** PySpark (Fabric Notebooks) & SQL Script
- **Storage:** OneLake (Delta Lake format)
- **Source Systems:** SQL Server 2022, Snowflake, GitHub raw files
- **Serving:** Fabric Warehouse (T-SQL), DirectLake Semantic Model
- **Version Control:** GitHub (Fabric Git integration)

## Key Engineering Patterns

- Incremental watermark loads (`updated_at`) for SQL Server and Snowflake sources
- Full load pattern for file-based sources (payments, HR)
- Schema evolution handling in Silver transformation
- Pipe-delimited addon column exploded into child table in Silver
- Star schema with `dim_date`, `dim_customer`, `dim_vehicle`,
  `dim_branch`, `fact_rental`, `fact_payment`, `fact_maintenance`

## Repository Structure

```
DriveZA-Holdings/
├── .git/
├── .gitignore
├── .vscode/
├── architecture/
│   ├── DriveZa Architecture.drawio.svg
│   ├── DriveZa Architecture.drawio.png
│   └── Architecture Diagram.png
├── data/
│   ├── README.md
│   └── raw-landing/
│       └── admin/
│           ├── crm_branches.csv
│           └── hr_staff.csv
├── docs/
├── Fabric/
│   ├── Bronze/
│   │   ├── Notebooks/
│   │   │   ├── NB_Bronze_Admin_Files_Loading.Notebook/
│   │   │   ├── NB_Bronze_Crm_GetWatermark.Notebook/
│   │   │   ├── NB_Bronze_Crm_RecordFailure.Notebook/
│   │   │   ├── NB_Bronze_Crm_SchemaEvolutionCheck.Notebook/
│   │   │   ├── NB_Bronze_Crm_UpdateWatermark.Notebook/
│   │   │   ├── NB_Bronze_DataQuality.Notebook/
│   │   │   └── NB_Bronze_Meta_Setup.Notebook/
│   │   ├── Pipelines/
│   │   │   ├── PL_Bronze_Admin.DataPipeline/
│   │   │   ├── PL_Bronze_CRM.DataPipeline/
│   │   │   └── PL_Bronze_QualityCheck.DataPipeline/
│   │   └── Strorage/
│   │       ├── DRIVEZA_FLEET.MirroredDatabase/
│   │       └── LH_DRZ_BRONZE.Lakehouse/
│   ├── Silver/
│   │   ├── Notebooks/
│   │   │   ├── NB_Silver_MetaSetup.Notebook/
│   │   │   └── NB_Silver_Transformation.Notebook/
│   │   ├── Pipelines/
│   │   │   └── PL_Silver_Transform.DataPipeline/
│   │   └── Storage/
│   │       └── LH_DRZ_SILVER.Lakehouse/
│   ├── Gold/
│   │   ├── Notebooks/
│   │   │   └── NB_Gold_DateTableGeneration.Notebook/
│   │   ├── Pipelines/
│   │   │   ├── PL_Gold_DimDate.DataPipeline/
│   │   │   └── PL_Gold_Transformation.DataPipeline/
│   │   └── Storage/
│   │       └── WH_DRZ_GOLD.Warehouse/
│   └── Readme.md
├── logs/
│   └── query_log.sql
├── README.md
├── src/
│   ├── snowflake/
│   │   ├── ddl/
│   │   │   └── DriveZa_Fleet_ddl(snowflake).ipynb
│   │   └── loaders/
│   │       └── DriveZa_Fleet_Tables_load(Snowflake).ipynb
│   └── sql_server/
│       ├── ddl/
│       │   └── DriveZa_Crm_Tables_ddl(sqlserver).sql
│       └── loaders/
│           └── DriveZa_Crm_Tables_load(sqlserver).py
└── .git/
```

## Setup Guide

See [docs/setup_guide.md](docs/setup_guide.md) for full instructions
to reproduce this project from scratch.

## Data

All data is synthetic and generated using Python (Faker + custom generators).
Generation script: `src/data_generation/generate_data.py`