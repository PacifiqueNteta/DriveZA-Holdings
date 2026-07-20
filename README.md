# DriveZA Holdings — Data Platform

 End-to-end data engineering solution simulating a project done for a client built on Microsoft Fabric following medallion architecture (Bronze → Silver → Gold).

## Company Overview

DriveZA Holdings (Pty) Ltd is a fictional South African vehicle rental company
operating across 9 provinces, 25+ cities, and 32 branches with a fleet of 400
vehicles. This project builds a production-grade data platform ingesting from
multiple source systems into a governed, versioned lakehouse.

## Architecture

![Architecture Diagram](architecture/Architecture%20Diagram.png)

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
- **Transformation:** PySpark (Fabric Notebooks)
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

```text
Driveza-holdings-data-platform/
│
│
├── architecture/
│   ├── architecture_diagram.png
│   ├── data_flow.md
│   └── design_decisions.md
│
├── data/
│   │
│   └── raw-landing/
│       ├── admin/
│           ├── crm_branches.csv
│           └── hr_staff.csv
│
├── src/
│   ├── sql_server/
│   │   ├── ddl/
│   │   │   └── driveza_crm_ddl.sql
│   │   └── loaders/
│   │       └── load_sqlserver.py
│   └── snowflake/
│       ├── ddl/
│       │   └── driveza_fleet_ddl.sql
│       └── loaders/
│           └── load_snowflake.sql
│
├── docs/
│   ├── setup_guide.md
│   ├── watermark_strategy.md
│   ├── star_schema.md
│   └── source_systems.md
│
└── fabric/
    ├── Bronze
    │       ├── Notebooks
    │       ├── Pipelines
    │       └── Lakehouse
    │
    │
    │
    ├── Silver
    └── Gold
```

## Setup Guide

See [docs/setup_guide.md](docs/setup_guide.md) for full instructions
to reproduce this project from scratch.

## Data

All data is synthetic and generated using Python (Faker + custom generators).
Generation script: `src/data_generation/generate_data.py`