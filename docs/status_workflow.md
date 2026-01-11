# Artifact Status and Processing Workflow

This document outlines the lifecycle of data ingestion in the application, from S3 upload to domain model creation.

```mermaid
graph TD
    A[S3 Upload] -->|Event Trigger| B(Process S3 File Task)
    B -->|Ingest| C{RawData Creation}
    C -->|Success| D[Artifact Status: COMPLETED]
    C -->|Failure| E[Artifact Status: FAILED]
    D -->|Trigger| F(Process Artifact Task)
    F -->|Process Rows| G{Strategy Execution}
    G -->|Valid| H[RawData: PROCESSED]
    H --> I[Domain Model Created]
    G -->|Invalid| J[RawData: FAILED]
```

## Lifecycle States

### Artifact Status
| Status | Description |
| :--- | :--- |
| **PENDING** | Initial state (transient). |
| **PROCESSING** | File is being read and parsed into `RawData`. |
| **COMPLETED** | file successfully ingested into `RawData` table. Ready for processing. |
| **FAILED** | Ingestion failed (e.g., malformed CSV, empty file). |

### RawData Status
| Status | Description |
| :--- | :--- |
| **PENDING** | Row parsed from CSV, waiting for strategy application. |
| **PROCESSED** | Successfully transformed and loaded into domain model (e.g., `PharmacyClaim`). |
| **FAILED** | validation or transformation error occurred. See `error_message`. |
