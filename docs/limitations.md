# Limitations and Future Improvements

- Source systems and data are synthetic; production use would require real connection, credential, and network configuration.
- Bronze CSV configuration is intentionally simple for the project, but production control should use typed, governed Delta or warehouse tables with validation and audit history.
- Failure-event publishing is wired into the Silver transformation but disabled until the required Business Event schema and Activator configuration are provisioned and tested.
- Schema evolution is passive in the current Bronze solution. A direct source schema-evolution check was not completed because the notebook-based ODBC/JDBC connection was unreliable in this environment. The attempted schema-check notebook was therefore failing and is not used in the CRM pipeline. Source changes require investigation, manual updates, and a pipeline rerun before processing resumes, which can increase downtime and operational cost. Production improvements should use approved schema snapshots or contracts, classify compatible and breaking changes, and route incompatible changes through quarantine, notification, and controlled approval workflows.
- Purview policies, sensitivity labels, access groups, Power BI reports, Data Agent, and Copilot settings are service-managed rather than stored here.
- Automated unit, integration, and data-reconciliation tests should be added around each source and layer.
- Future improvements include CI/CD validation gates, richer data-quality thresholds, row-level reconciliation, referential-integrity checks, freshness monitoring, late-arriving fact handling, replay and backfill procedures, production-scale performance testing, incremental refresh optimization, secret externalization, and disaster-recovery planning.
