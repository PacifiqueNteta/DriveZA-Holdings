# Data Sources

DriveZA integrates four source feeds. The production-style systems and their project substitutes are:

- Bluebird Auto Rental Systems on on-premises SQL Server: customers, payments, rentals, promotions, and reviews. This is represented by the SQL Server CRM source.
- MiX by Powerfleet fleet data landing in a fleet-managed Azure SQL Server: vehicles, maintenance, and incidents. This is represented by Snowflake and mirrored into Fabric as `DRIVEZA_FLEET`.
- Branch Operations spreadsheet: branch reference, manager assignment, and fleet capacity data. This is represented by a GitHub-hosted CSV.
- PaySpace HR export spreadsheet: employee reference data. This is represented by a GitHub-hosted CSV.

The CRM processing uses a Self-hosted Integration Runtime (SHIR). The fleet source is represented through the mirrored `DRIVEZA_FLEET` database in Fabric, while the file sources use an HTTP connector.

The substitutes are intentional: Snowflake demonstrates a mirrored/shared external database pattern, and GitHub-hosted CSV files simulate periodic spreadsheet exports without requiring access to the fictional production systems.
