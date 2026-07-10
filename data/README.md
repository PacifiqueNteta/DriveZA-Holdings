# Raw Landing Data

This folder simulates two file-based source systems:

| File | Simulates |
|---|---|
| `payments/crm_payments.csv` | Daily settlement file dropped by Peach Payments gateway via SFTP |
| `hr/hr_staff.csv` | Weekly HR system export from Sage 300 People via SFTP |

In production these files would land in an Azure Data Lake Storage container
or an SFTP server. For this portfolio project they are hosted here as raw
GitHub URLs and ingested into Bronze via Fabric's HTTP connector.

**Raw URLs used in Fabric pipelines:**
- `https://raw.githubusercontent.com/[username]/driveza-data-platform/main/data/raw-landing/payments/crm_payments.csv`
- `https://raw.githubusercontent.com/[username]/driveza-data-platform/main/data/raw-landing/hr/hr_staff.csv`

All data is fully synthetic and generated for portfolio purposes only.