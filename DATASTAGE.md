# DataStage OpenLineage Integration

## Problem Statement

IBM DataStage does not natively emit OpenLineage events. There is no plugin system, no webhook mechanism, and no built-in lineage export in the OpenLineage standard format.

This means DataStage jobs are **black boxes** from a data governance perspective: data flows through ETL pipelines but there is no standardized way to track what tables are read, what tables are written, how many rows are processed, or whether a job succeeded or failed — at least not in a format compatible with modern data catalogs and lineage tools.

This project bridges that gap by providing **external pollers** that observe DataStage job executions and emit standard [OpenLineage](https://openlineage.io) events to any compatible backend (Marquez, Atlan, Purview, Dataplex, etc.).

## Two Modules, Two Deployment Targets

We provide two independent modules because DataStage exists in two very different deployment models:

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   datastage-openlineage          datastage-dsjob-openlineage        │
│   ─────────────────────          ───────────────────────────        │
│   Cloud Pak for Data             On-premise DataStage 11.7          │
│   (SaaS or on-prem CPD)         (legacy Server)                    │
│                                                                     │
│   REST API polling               dsjob CLI polling                  │
│   IAM token auth                 Local process execution            │
│   Flow definitions → datasets    .dsx file parsing → datasets       │
│   Runs anywhere (network)        Runs on DS Engine machine          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**For a client running DataStage 11.7 on-premise, the relevant module is `datastage-dsjob-openlineage`.**

---

## Module: datastage-dsjob-openlineage (DataStage 11.7 On-Premise)

### Overview

A standalone Spring Boot application that runs on the DataStage Engine server. It polls job statuses via the `dsjob` command-line tool every 30 seconds and emits OpenLineage events for each job execution.

### Architecture

```
┌────────────────────────────────────────────────────────────┐
│                  DataStage Engine Server                    │
│                                                            │
│   ┌──────────────┐        ┌──────────────────────────┐     │
│   │  DataStage    │        │  datastage-dsjob-        │     │
│   │  Engine 11.7  │◄──────│  openlineage (Java app)  │     │
│   │               │ dsjob  │                          │     │
│   │  - Jobs       │ CLI    │  DsjobRunner             │     │
│   │  - Projects   │        │  DsxParser               │     │
│   │  - dsenv      │        │  DsjobPoller             │     │
│   └──────────────┘        │  DataStageEventEmitter   │     │
│                            └─────────┬────────────────┘     │
│                                      │                      │
└──────────────────────────────────────┼──────────────────────┘
                                       │ HTTP / Console
                                       ▼
                              ┌──────────────────┐
                              │  OpenLineage     │
                              │  Backend         │
                              │  (Marquez, etc.) │
                              └──────────────────┘
```

### What dsjob Commands Are Used

| Command | Purpose | Data extracted |
|---|---|---|
| `dsjob -ljobs <project>` | List all jobs in a project | Job names |
| `dsjob -jobinfo <project> <job>` | Get status of last/current run | Status code, wave number, start/end timestamps |
| `dsjob -report <project> <job>` | Get execution report | Row counts per stage (rows read, rows written) |

### What Data We Capture

| Data point | Source | OpenLineage mapping |
|---|---|---|
| Job name | `dsjob -ljobs` | `job.name` = `project.jobName` |
| Run status | `dsjob -jobinfo` → status code | Event type: START / RUNNING / COMPLETE / FAIL |
| Run identity | `dsjob -jobinfo` → wave number | Unique run = `project:job:waveNumber` |
| Start/end time | `dsjob -jobinfo` | `eventTime` on START and COMPLETE events |
| Row counts per stage | `dsjob -report` | `outputStatistics` facet (rowCount) |
| **Source tables** | `.dsx` file parsing | `inputs[]` with namespace=`db2://host:port/db`, name=`TABLE` |
| **Target tables** | `.dsx` file parsing | `outputs[]` with namespace=`oracle://host:port/db`, name=`TABLE` |
| **File paths** | `.dsx` file parsing | `inputs[]`/`outputs[]` with namespace=`file://path` |
| **Connection details** | `.dsx` file parsing | Namespace = `scheme://server:port/database` |

### What We CANNOT Capture (Limitations)

| Limitation | Why | Workaround |
|---|---|---|
| **Only the last run** is visible | `dsjob -jobinfo` returns only the current/last run. If a job starts and finishes between two polls, we see the result but miss the START event timing. | Reduce poll interval (e.g. 10s). The state machine still emits START+COMPLETE for completed runs. |
| **No column-level lineage** | `dsjob` does not expose column mappings. | Could be extracted from `.dsx` files in a future enhancement (transformer stage definitions). |
| **No real-time streaming** | Polling-based, not event-driven. There is a 0-30s delay between a status change and its detection. | Acceptable for batch ETL lineage. |
| **Dataset names require .dsx exports** | Without `.dsx` files, datasets are identified by stage name only (e.g. `project.job.DB2_Target`) instead of real table names (e.g. `CUSTOMERS`). | Export `.dsx` files from DataStage Designer. Can be automated via script. |
| **No sequence/dependency tracking** | We don't capture job scheduling sequences or dependencies between jobs. | Could be added by parsing DataStage Sequence jobs in `.dsx` files. |

### Polling State Machine

Each job run (identified by `project:jobName:waveNumber`) goes through this state machine:

```
                  ┌─────────────────────────────────┐
                  │          dsjob -jobinfo          │
                  │          returns status           │
                  └────────────┬────────────────────┘
                               │
                               ▼
    ┌─────────┐          ┌───────────┐
    │ Queued  │          │   NONE    │ (first time we see this wave number)
    │ (13)    │──skip──►│           │
    └─────────┘          └─────┬─────┘
                               │
              ┌────────────────┼────────────────────┐
              │                │                    │
              ▼                ▼                    ▼
        Running (0)     Completed (1,2)       Failed (3,9)
              │                │                    │
              ▼                ▼                    ▼
        emit START       emit START            emit START
              │          emit COMPLETE          emit FAIL
              ▼                │                    │
         [STARTED]             ▼                    ▼
              │           [TERMINAL]            [TERMINAL]
              │
     ┌────────┼────────────────┐
     │        │                │
     ▼        ▼                ▼
  Running  Completed        Failed
     │        │                │
     ▼        ▼                ▼
  emit     emit              emit
  RUNNING  COMPLETE          FAIL
     │        │                │
     ▼        ▼                ▼
  [RUNNING] [TERMINAL]     [TERMINAL]
```

Runs in TERMINAL state are evicted from memory after 2 hours (configurable).

### dsjob Status Codes Reference

| Code | Text | Meaning | Our action |
|---|---|---|---|
| 0 | Running | Job is currently executing | Emit START (first time) or RUNNING |
| 1 | RUN OK | Job completed successfully | Emit COMPLETE |
| 2 | RUN with warnings | Job completed with warnings | Emit COMPLETE |
| 3 | RUN FAILED | Job failed | Emit FAIL |
| 9 | Stopped | Job was manually stopped | Emit FAIL |
| 13 | Queued | Job is waiting to run | Skip (wait) |
| 4-8, 10-12 | Various | Validated, reset, not compiled, etc. | Skip (not actionable) |

### DSX Parser — Extracting Real Dataset Names

#### What is a .dsx file?

A `.dsx` file is a text-based export format from the DataStage Designer. It contains the full definition of a job: stages, connections, properties, column mappings, and links.

#### How to export .dsx files

From DataStage Designer:
1. Right-click a job (or select multiple jobs)
2. Export → DataStage Components
3. Save as `.dsx` file

Or export an entire project at once.

#### What the parser extracts

From each connector stage in the `.dsx` file:

```
BEGIN DSRECORD
   Name "Oracle_Target"              ← stage name (for matching with dsjob -report)
   StageType "OracleConnectorPX"     ← connector type → namespace scheme (oracle://)
   InputPins "1"                     ← has inputs → this is a TARGET (OUTPUT dataset)
   OutputPins "0"
   Properties "...                   ← contains table name, server, port, database
      TableName=CUSTOMERS
      Server=oracledb.company.com
      Port=1521
      Database=WAREHOUSE
   ..."
END DSRECORD
```

This becomes the OpenLineage dataset:
```json
{
  "namespace": "oracle://oracledb.company.com:1521/WAREHOUSE",
  "name": "CUSTOMERS",
  "type": "OUTPUT"
}
```

#### Supported connector types

| DataStage Stage Type | Namespace Scheme | Example namespace |
|---|---|---|
| `DB2ConnectorPX`, `PxDB2`, `CDB2Stage` | `db2://` | `db2://dbserver:50000/WAREHOUSE` |
| `OracleConnectorPX`, `COracleStage` | `oracle://` | `oracle://oraserver:1521/PROD` |
| `TeradataConnectorPX` | `teradata://` | `teradata://tdserver/DW` |
| `NetezzaConnectorPX` | `netezza://` | `netezza://nzserver/ANALYTICS` |
| `PostgreSQLConnectorPX` | `postgresql://` | `postgresql://pgserver:5432/mydb` |
| `MySQLConnectorPX` | `mysql://` | `mysql://myserver:3306/mydb` |
| `SQLServerConnectorPX` | `sqlserver://` | `sqlserver://sqlsrv:1433/DW` |
| `ODBCConnectorPX`, `CODBCStage` | `odbc://` | `odbc://server/DSN` |
| `PxSequentialFile`, `CSequentialFileStage` | `file://` | `file://localhost` |
| `PxDataSet`, `CDataSetStage` | `file://` | `file://localhost` |

### Example OpenLineage Events

#### START event (job begins running)

```json
{
  "eventType": "START",
  "eventTime": "2025-03-15T10:30:00Z",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000",
    "facets": {
      "nominalTime": {
        "nominalStartTime": "2025-03-15T10:30:00Z"
      }
    }
  },
  "job": {
    "namespace": "datastage",
    "name": "production.ETL_Customer_Load"
  },
  "inputs": [
    {
      "namespace": "file://localhost",
      "name": "/data/incoming/customers.csv"
    }
  ],
  "outputs": [
    {
      "namespace": "db2://dbserver:50000/WAREHOUSE",
      "name": "CUSTOMERS"
    }
  ]
}
```

#### COMPLETE event (job finishes with row counts)

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-03-15T10:35:00Z",
  "run": {
    "runId": "550e8400-e29b-41d4-a716-446655440000"
  },
  "job": {
    "namespace": "datastage",
    "name": "production.ETL_Customer_Load"
  },
  "inputs": [
    {
      "namespace": "file://localhost",
      "name": "/data/incoming/customers.csv"
    }
  ],
  "outputs": [
    {
      "namespace": "db2://dbserver:50000/WAREHOUSE",
      "name": "CUSTOMERS",
      "outputFacets": {
        "outputStatistics": {
          "rowCount": 15000
        }
      }
    }
  ]
}
```

### Configuration Reference

```yaml
datastage:
  # Path to the DSEngine installation directory
  ds-home: /opt/IBM/InformationServer/Server/DSEngine

  # List of DataStage projects to monitor (supports multiple)
  projects:
    - production
    - staging

  # Optional: directory containing .dsx export files for dataset name resolution
  # Without this, datasets are identified by stage name only
  dsx-directory: /opt/datastage/exports

  polling:
    # How often to poll (ISO 8601 duration)
    interval: PT30S

    # How long to keep completed runs in memory before eviction
    run-retention: PT2H

openlineage:
  # Enable/disable OpenLineage emission
  enabled: true

  # Namespace for all jobs (appears in job.namespace in events)
  namespace: datastage

  # Transport: console (stdout), http (send to backend), noop (disabled)
  transport: http

  http:
    # URL of the OpenLineage backend
    url: http://lineage-backend:5000/api/v1/lineage

    # Optional API key
    api-key: ${OL_API_KEY:}
```

### Deployment

#### Prerequisites

1. **Java 17+** installed on the DataStage Engine server
2. **`dsjob`** accessible — typically at `$DSHOME/bin/dsjob`
3. **Network access** from the Engine server to the OpenLineage backend (if using HTTP transport)

#### Installation

```bash
# 1. Copy the JAR to the DataStage Engine server
scp datastage-dsjob-openlineage-0.1.0-SNAPSHOT.jar dsadmin@ds-server:/opt/ol-poller/

# 2. Create a startup script
cat > /opt/ol-poller/start.sh << 'SCRIPT'
#!/bin/bash
source /opt/IBM/InformationServer/Server/DSEngine/dsenv
java -jar /opt/ol-poller/datastage-dsjob-openlineage-0.1.0-SNAPSHOT.jar \
  --datastage.projects=production,staging \
  --datastage.dsx-directory=/opt/datastage/exports \
  --openlineage.transport=http \
  --openlineage.http.url=http://lineage-backend:5000/api/v1/lineage
SCRIPT
chmod +x /opt/ol-poller/start.sh

# 3. Run it
/opt/ol-poller/start.sh
```

#### As a systemd service (recommended for production)

```ini
[Unit]
Description=DataStage OpenLineage Poller
After=network.target

[Service]
Type=simple
User=dsadmin
ExecStart=/opt/ol-poller/start.sh
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

---

## Module: datastage-openlineage (Cloud Pak for Data)

This module targets DataStage running on IBM Cloud Pak for Data (SaaS or on-prem CPD deployment). It uses the CPD REST API instead of the `dsjob` CLI.

**This module is NOT relevant for DataStage 11.7 on-premise.** It is included for completeness and for future use if the client migrates to Cloud Pak for Data.

### Key differences from the dsjob module

| Aspect | dsjob module (11.7) | CPD module |
|---|---|---|
| Interface | `dsjob` CLI (ProcessBuilder) | REST API (HTTP) |
| Authentication | None (local process) | IBM Cloud IAM token |
| Dataset resolution | `.dsx` file parsing | Flow definition API |
| Deployment | On the DS Engine machine | Anywhere with network access |
| Project discovery | Configured project list | API-driven |

### Architecture

```
┌────────────────────┐           ┌──────────────────────────┐
│  Cloud Pak for     │           │  datastage-openlineage   │
│  Data platform     │◄─────────│  (Java app)              │
│                    │  REST API │                          │
│  - Jobs API        │           │  IamTokenManager         │
│  - Runs API        │           │  DataStageClient         │
│  - Flows API       │           │  FlowParser              │
│  - Connections API │           │  DataStagePoller          │
└────────────────────┘           │  DataStageEventEmitter   │
                                 └──────────┬───────────────┘
                                            │
                                            ▼
                                   OpenLineage Backend
```

### REST API Endpoints Used

| Endpoint | Purpose |
|---|---|
| `POST /identity/token` (IAM) | Authenticate with API key |
| `GET /v2/jobs?project_id=` | List all DataStage jobs |
| `GET /v2/jobs/{id}/runs?project_id=` | List runs for a job |
| `GET /v2/jobs/{id}/runs/{runId}?project_id=` | Get run detail (status, metrics) |
| `GET /v3/data_intg/flows/{flowId}?project_id=` | Get flow definition (stages, connections) |
| `GET /v2/connections/{connId}?project_id=` | Get connection details (host, port, db) |

### What Data We Capture (CPD module)

Same as the dsjob module, plus:

| Additional data | Source | Notes |
|---|---|---|
| Full flow definitions | Flows API | No .dsx export needed — datasets extracted automatically |
| Connection strings | Connections API | Resolved live from the platform |
| Run history | Runs API | Multiple runs visible per job (not just the last one) |
| Stage-level metrics | Run detail API | Row counts per stage, bytes processed |

---

## Questions to Discuss in the Meeting

### Must-answer questions for the DataStage 11.7 module

1. **Where is DataStage Engine installed?**
   - We need `$DSHOME` path (default: `/opt/IBM/InformationServer/Server/DSEngine`)
   - Verify `dsjob` is functional: `source dsenv && dsjob -ljobs <project>`

2. **What are the project names?**
   - List of DataStage projects to monitor

3. **How many jobs per project?**
   - Helps estimate poll frequency. 100+ jobs may need a longer interval.

4. **Can we install Java 17 on the Engine server?**
   - Required for the Spring Boot poller

5. **Can we export .dsx files?**
   - Needed for real dataset names (table names, connection details)
   - Can the team export all jobs periodically? Or provide a one-time export?

6. **What connector types are used?**
   - DB2, Oracle, Teradata, files, etc.?
   - Helps validate our connector mapping is complete

7. **What is the target OpenLineage backend?**
   - Marquez? Atlan? Purview? Custom?
   - Needed for HTTP transport configuration

8. **Network: can the Engine server reach the lineage backend?**
   - If not, we can use console transport and ship logs separately

### Optional: future enhancements to discuss

9. **Are there DataStage Sequence jobs?**
   - Sequences define job dependencies (job A → job B → job C)
   - We could parse these for parent-child lineage

10. **Is there interest in column-level lineage?**
    - Possible by parsing transformer stage definitions in `.dsx` files
    - Significantly more complex

11. **Is there a migration plan to Cloud Pak for Data?**
    - If yes, the CPD module (`datastage-openlineage`) would become relevant
    - Both modules can coexist during migration

12. **Are there other tools in the data pipeline (BusinessObjects, etc.)?**
    - We can extend the approach to cover BI tools with a similar poller pattern

---

## Project File Structure

```
datastage-dsjob-openlineage/
├── pom.xml
└── src/main/
    ├── java/io/openlineage/datastage/dsjob/
    │   ├── DataStageDsjobApplication.java          # Entry point (@SpringBootApplication)
    │   ├── config/
    │   │   ├── DsjobProperties.java                # datastage.* configuration
    │   │   ├── OpenLineageEmitterProperties.java    # openlineage.* configuration
    │   │   └── DsjobOpenLineageConfiguration.java   # Spring @Bean wiring
    │   ├── command/
    │   │   ├── DsjobRunner.java                     # ProcessBuilder wrapper for dsjob CLI
    │   │   ├── JobInfo.java                         # Parsed output of dsjob -jobinfo
    │   │   └── JobReport.java                       # Parsed output of dsjob -report
    │   ├── dsx/
    │   │   ├── DsxParser.java                       # Parses .dsx files → datasets
    │   │   ├── DatasetDescriptor.java               # Record: namespace, name, type, stageName
    │   │   └── ConnectorType.java                   # Stage type → namespace scheme mapping
    │   ├── polling/
    │   │   ├── DsjobPoller.java                     # @Scheduled poll loop + state machine
    │   │   ├── TrackedRun.java                      # Per-run state: NONE→STARTED→RUNNING→TERMINAL
    │   │   └── RunStateTracker.java                 # ConcurrentHashMap-based run tracking
    │   └── event/
    │       └── DataStageEventEmitter.java           # Builds & emits OpenLineage RunEvents
    └── resources/
        └── application.yml                          # Default configuration

datastage-openlineage/
├── pom.xml
└── src/main/
    ├── java/io/openlineage/datastage/
    │   ├── DataStageOpenLineageApplication.java     # Entry point
    │   ├── config/
    │   │   ├── DataStageProperties.java             # datastage.* configuration (apiKey, serviceUrl)
    │   │   ├── OpenLineageEmitterProperties.java    # openlineage.* configuration
    │   │   └── DataStageOpenLineageConfiguration.java
    │   ├── client/
    │   │   ├── IamTokenManager.java                 # IBM Cloud IAM token refresh
    │   │   ├── DataStageClient.java                 # REST client (jobs, runs, flows, connections)
    │   │   └── model/
    │   │       ├── JobListResponse.java
    │   │       ├── JobRunListResponse.java
    │   │       ├── JobRunDetail.java
    │   │       ├── FlowDefinition.java
    │   │       └── ConnectionDetail.java
    │   ├── flow/
    │   │   ├── FlowParser.java                      # Flow definition → datasets
    │   │   ├── DatasetDescriptor.java
    │   │   └── ConnectorType.java
    │   ├── polling/
    │   │   ├── DataStagePoller.java
    │   │   ├── TrackedRun.java
    │   │   └── RunStateTracker.java
    │   └── event/
    │       └── DataStageEventEmitter.java
    └── resources/
        └── application.yml
```

## Building

```bash
# Build everything
mvn clean install

# Build only the dsjob module (for DS 11.7)
mvn clean install -pl datastage-dsjob-openlineage -am

# Build only the CPD module
mvn clean install -pl datastage-openlineage -am
```
