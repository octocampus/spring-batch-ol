# DataStage OpenLineage Integration

## Problem Statement

IBM DataStage does not natively emit OpenLineage events. There is no plugin system, no webhook mechanism, and no built-in lineage export in the OpenLineage standard format.

This means DataStage jobs are **black boxes** from a data governance perspective: data flows through ETL pipelines but there is no standardized way to track what tables are read, what tables are written, how many rows are processed, or whether a job succeeded or failed — at least not in a format compatible with modern data catalogs and lineage tools.

This project bridges that gap by providing **external pollers** that observe DataStage job executions and emit standard [OpenLineage](https://openlineage.io) events to any compatible backend (Marquez, Atlan, Purview, Dataplex, etc.).

---

## All Available Data Sources in DataStage 11.7

DataStage 11.7 on-premise exposes data through **7 different interfaces**. Each provides different pieces of the lineage puzzle:

### Complete Inventory

| # | Interface | What it provides | Runtime events | Dataset names | Row counts | Column lineage | Run history | Deployment constraint |
|---|---|---|---|---|---|---|---|---|
| 1 | **`dsjob` CLI** | Job status, wave number, timestamps, report | Current/last run only | No | Per stage | No | No | Must run on DS Engine server |
| 2 | **`.dsx` file export** | Job definitions: stages, connections, properties | No (static) | **Yes** (tables, files) | No | No | No | Export from Designer (manual or scripted) |
| 3 | **Operational Metadata DB (DSODB)** | Full run history, row counts per stage/link | **All runs** | No | **Per stage AND link** | No | **Yes** | JDBC access to DSODB database |
| 4 | **IGC REST API** | Asset catalog, lineage (table + column level) | No (static) | **Yes** (automatic) | No | **Yes** | No | IGC must be installed and scanned |
| 5 | **xmeta Repository DB** | Job/stage definitions, connections, data sources | No (static) | **Yes** | No | Partial | No | SQL access to xmeta DB2 database |
| 6 | **`istool` CLI** | Metadata export (`.isx` XML format) | No (static) | **Yes** | No | **Yes** | No | Must run on IS server |
| 7 | **Director Logs (files)** | Detailed run logs, error messages, timestamps | Per run | No | Yes (in log text) | No | **Yes** (on disk) | Access to filesystem on DS server |
| 8 | **DSAPI (C native)** | Same as dsjob (programmatic) | Current/last run | No | Per stage | No | No | JNI/JNA — not recommended |

### Key Takeaway

No single interface provides everything. The best approach **combines** multiple sources:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  RUNTIME EVENTS          DATASET NAMES           ROW COUNTS             │
│  (when did it run?)      (what tables/files?)    (how many rows?)       │
│                                                                         │
│  ┌─────────────────┐     ┌──────────────────┐    ┌──────────────────┐   │
│  │ Option A: dsjob │     │ Option A: .dsx   │    │ Option A: dsjob  │   │
│  │ (last run only) │     │ (manual export)  │    │ -report          │   │
│  ├─────────────────┤     ├──────────────────┤    ├──────────────────┤   │
│  │ Option B: DSODB │     │ Option B: IGC    │    │ Option B: DSODB  │   │
│  │ (all runs) ★    │     │ (auto, +columns) │    │ (per link) ★     │   │
│  ├─────────────────┤     ├──────────────────┤    └──────────────────┘   │
│  │ Option C: Logs  │     │ Option C: xmeta  │                          │
│  │ (parse files)   │     │ (SQL queries)    │                          │
│  └─────────────────┘     └──────────────────┘                          │
│                                                                         │
│  ★ = recommended when available                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Interface Details

### 1. `dsjob` CLI

**What:** Command-line tool installed with DataStage Engine at `$DSHOME/bin/dsjob`.

**Commands used:**

```bash
# List all jobs in a project
dsjob -ljobs <project>

# Get status of current/last run
dsjob -jobinfo <project> <job>
# Output:
#   Job Status    : RUN OK (1)
#   Job Controller: ENGINE_SERVER
#   Last Run Start: 2025-03-15 10:30:00
#   Last Run End  : 2025-03-15 10:35:12
#   Job Wave No   : 456

# Get execution report (row counts)
dsjob -report <project> <job>
# Output:
#   === Stage: DB2_Source ===
#     Rows Read   : 15000
#     Rows Written: 15000
#   === Stage: Transformer ===
#     Rows Read   : 15000
#     Rows Written: 14800
#   === Stage: Oracle_Target ===
#     Rows Read   : 14800
#     Rows Written: 14800

# List stages in a job
dsjob -lstages <project> <job>
```

**Status codes:**

| Code | Text | Meaning |
|---|---|---|
| 0 | Running | Job is currently executing |
| 1 | RUN OK | Completed successfully |
| 2 | RUN with warnings | Completed with warnings |
| 3 | RUN FAILED | Failed |
| 9 | Stopped | Manually stopped |
| 13 | Queued | Waiting to run |
| 4-8, 10-12 | Various | Validated, reset, not compiled (not actionable) |

**Pros:** Simple, always available, no setup needed.
**Cons:** Only sees the last/current run. If a job completes between two polls, we miss the exact start time. Text output requires parsing.

---

### 2. `.dsx` File Export

**What:** Text-based export format from DataStage Designer containing full job definitions.

**How to get them:**
- DataStage Designer → right-click job → Export → DataStage Components → save as `.dsx`
- Can export entire projects at once
- Can potentially be scripted via `istool export`

**What they contain:**

```
BEGIN DSJOB
   Name "ETL_Customer_Load"

   BEGIN DSRECORD
      Name "DB2_Source"
      StageType "DB2ConnectorPX"         ← connector type
      InputPins "0"                      ← no inputs = source stage
      OutputPins "1"                     ← has output = reads data
      Properties "...
         TableName=CUSTOMERS             ← table name
         Server=dbserver.company.com     ← connection info
         Port=50000
         Database=WAREHOUSE
      ..."
   END DSRECORD

   BEGIN DSRECORD
      Name "Oracle_Target"
      StageType "OracleConnectorPX"
      InputPins "1"                      ← has input = target stage
      OutputPins "0"
      Properties "...
         TableName=CUSTOMER_DW
         Server=oracledb.company.com
         Port=1521
         Database=DW_PROD
      ..."
   END DSRECORD
END DSJOB
```

**Connector type → namespace mapping:**

| StageType in .dsx | Namespace scheme | Example |
|---|---|---|
| `DB2ConnectorPX`, `PxDB2`, `CDB2Stage` | `db2://` | `db2://dbserver:50000/WAREHOUSE` |
| `OracleConnectorPX`, `COracleStage` | `oracle://` | `oracle://oraserver:1521/PROD` |
| `TeradataConnectorPX`, `CTeradataStage` | `teradata://` | `teradata://tdserver/DW` |
| `NetezzaConnectorPX`, `CNetezzaStage` | `netezza://` | `netezza://nzserver/ANALYTICS` |
| `PostgreSQLConnectorPX` | `postgresql://` | `postgresql://pgserver:5432/mydb` |
| `MySQLConnectorPX` | `mysql://` | `mysql://myserver:3306/mydb` |
| `SQLServerConnectorPX` | `sqlserver://` | `sqlserver://sqlsrv:1433/DW` |
| `ODBCConnectorPX`, `CODBCStage` | `odbc://` | `odbc://server/DSN` |
| `PxSequentialFile`, `CSequentialFileStage` | `file://` | `file://localhost/path/to/file.csv` |
| `PxDataSet`, `CDataSetStage` | `file://` | `file://localhost/datasets/ds1` |

**Pros:** Rich metadata — table names, connection details, column definitions. No runtime dependency.
**Cons:** Static — must be re-exported when jobs change. Manual step (unless automated).

---

### 3. Operational Metadata Database (DSODB)

**What:** A relational database (DB2, Oracle, or SQL Server) where DataStage logs execution metadata after each run. Configured in DataStage Administrator → Operational Metadata.

**Key tables:**

| Table | Content | Key columns |
|---|---|---|
| `DSODB.JOBEXEC` | One row per job execution | `JOBNAME`, `PROJECTNAME`, `RUNSTARTTIME`, `RUNENDTIME`, `JOBSTATUS`, `WAVENO` |
| `DSODB.JOBRUN` | Run details | `JOBNAME`, `RUNTIMESTAMP`, `ELAPSEDTIME`, `STATUS` |
| `DSODB.STAGERESULT` | Row counts per stage | `JOBNAME`, `STAGENAME`, `WAVENO`, `ROWSREAD`, `ROWSWRITTEN`, `ROWSREJECTED` |
| `DSODB.LINKRESULT` | Row counts per link | `JOBNAME`, `STAGENAME`, `LINKNAME`, `WAVENO`, `ROWCOUNT` |

**Example queries:**

```sql
-- Get all runs for a job (full history!)
SELECT JOBNAME, PROJECTNAME, WAVENO, JOBSTATUS,
       RUNSTARTTIME, RUNENDTIME
FROM DSODB.JOBEXEC
WHERE PROJECTNAME = 'production'
ORDER BY RUNSTARTTIME DESC;

-- Get row counts for a specific run
SELECT STAGENAME, ROWSREAD, ROWSWRITTEN, ROWSREJECTED
FROM DSODB.STAGERESULT
WHERE JOBNAME = 'ETL_Customer_Load'
  AND WAVENO = 456;

-- Get link-level detail
SELECT STAGENAME, LINKNAME, ROWCOUNT
FROM DSODB.LINKRESULT
WHERE JOBNAME = 'ETL_Customer_Load'
  AND WAVENO = 456;

-- Detect new runs since last poll
SELECT *
FROM DSODB.JOBEXEC
WHERE RUNSTARTTIME > :lastPollTimestamp
  AND PROJECTNAME IN ('production', 'staging');
```

**Job status values in DSODB:**

| Value | Meaning |
|---|---|
| 1 | Running |
| 2 | Finished (OK) |
| 3 | Finished with warnings |
| 4 | Aborted (failed) |
| 5 | Stopped |

**Pros:**
- **Full run history** — we see ALL runs, not just the last one
- **Never miss a run** — even if it starts and completes between polls
- **Structured data** — SQL queries, no text parsing
- **Link-level row counts** — more granular than `dsjob -report`
- **Can run from any machine** with JDBC access (not tied to DS Engine server)
- **Timestamp-based polling** — query "runs since last poll" instead of scanning all jobs

**Cons:**
- Must be enabled in DataStage Administrator (already confirmed: **it is enabled**)
- No dataset names (still need `.dsx` or IGC for that)
- Table/column names may vary slightly between DS versions

---

### 4. Information Governance Catalog (IGC) REST API

**What:** Part of IBM Information Server, IGC is a metadata catalog that indexes DataStage assets and computes lineage. It has a REST API.

**Base URL:** `https://<is-server>:9443/ibm/iis/igc-rest/v1`

**Key endpoints:**

```
# Search for DataStage jobs
GET /search?types=dsjob&properties=name,short_description

# Get job details
GET /assets/{job_rid}

# Get lineage for a table (what reads/writes it)
GET /assets/{table_rid}/lineage

# Get column-level lineage
GET /assets/{column_rid}/lineage
```

**What it provides:**

| Data | Details |
|---|---|
| Job catalog | All DataStage jobs with descriptions |
| Table-level lineage | Source table → Job → Target table (automatic, no .dsx needed) |
| **Column-level lineage** | Source column → transformation → Target column |
| Connection details | Full connection strings for all data sources |
| Business metadata | Terms, classifications, stewards (if populated) |
| Impact analysis | "If I change column X, what downstream jobs are affected?" |

**Pros:**
- **Automatic dataset resolution** — no .dsx export needed
- **Column-level lineage** — the only source that provides this
- Rich metadata catalog with business context

**Cons:**
- Must be installed (separate component of Information Server)
- Must be configured to scan DataStage projects (IGC → "Discover" → "Automated Discovery")
- No runtime data (no row counts, no execution history)
- REST API can be slow on large catalogs

---

### 5. xmeta Repository Database

**What:** The shared metadata repository for IBM Information Server. Stored in DB2. Contains asset definitions for all Information Server components.

**Key tables for DataStage:**

| Table | Content |
|---|---|
| `IAVIEWS.DSJOB` | Job definitions |
| `IAVIEWS.DSSTAGE` | Stage definitions (type, properties) |
| `IAVIEWS.DSLINK` | Link definitions (connections between stages) |
| `IAVIEWS.DSCONNECTION` | Data connection details |

**Pros:** Complete job/stage definitions accessible via SQL. Alternative to `.dsx` parsing.
**Cons:** Undocumented schema (IBM internal). Complex joins. Read-only access required — accidental writes could corrupt the repository.

---

### 6. `istool` CLI

**What:** IBM Information Server command-line tool for metadata import/export.

```bash
# Export a DataStage job as .isx (XML)
istool export \
  -domain https://is-server:9443 \
  -u admin -p password \
  -archive /tmp/job_export.isx \
  -ds "project/ETL_Customer_Load"

# Export an entire project
istool export \
  -domain https://is-server:9443 \
  -u admin -p password \
  -archive /tmp/project_export.isx \
  -ds "production/*"
```

**Pros:** Can automate `.dsx`/`.isx` exports (no manual Designer step). XML format is easier to parse than `.dsx`.
**Cons:** Requires Information Services Director running. Needs admin credentials.

---

### 7. Director Logs (Filesystem)

**What:** Text log files written by DataStage for each job execution.

**Location:** `$DSHOME/../Logs/<project>/<job_name>/`

**Content:**
```
2025-03-15 10:30:00  Starting job ETL_Customer_Load, wave 456
2025-03-15 10:30:01  Stage DB2_Source: Starting
2025-03-15 10:30:05  Stage DB2_Source: 15000 rows read
2025-03-15 10:35:10  Stage Oracle_Target: 14800 rows written
2025-03-15 10:35:12  Job finished successfully
```

**Pros:** Full history (files persist on disk). Contains error messages and detailed timing.
**Cons:** Unstructured text. Format varies. Files can be rotated/deleted by admin.

---

## Recommended Approach Combinations

### Option A: `dsjob` + `.dsx` (currently implemented)

```
dsjob CLI ──── runtime events (last run), row counts
    +
.dsx files ─── dataset names, connection details
    │
    ▼
OpenLineage events
```

**Best for:** Quick start, minimal prerequisites.
**Limitations:** Only sees the last run per job. Must export `.dsx` files manually.

---

### Option B: DSODB + `.dsx` (recommended upgrade)

```
DSODB (SQL) ── full run history, row counts per link ★
    +
.dsx files ──── dataset names, connection details
    │
    ▼
OpenLineage events
```

**Best for:** Production deployment. Reliable, complete run history.
**Advantages over Option A:**
- Never miss a run (query by timestamp, not poll last status)
- Runs from any machine with JDBC access (not tied to DS Engine)
- Structured SQL data (no text parsing)
- Link-level row counts (more granular)

---

### Option C: DSODB + IGC (best possible)

```
DSODB (SQL) ── full run history, row counts per link ★
    +
IGC REST API ── dataset names, connections, column-level lineage ★
    │
    ▼
OpenLineage events (including column-level lineage facets)
```

**Best for:** Full governance. Provides everything including column-level lineage.
**Requirements:** IGC must be installed and have scanned DataStage projects.

---

### Option D: DSODB + `istool` export (automated .dsx alternative)

```
DSODB (SQL) ── full run history, row counts per link ★
    +
istool CLI ──── automated .isx export → dataset names
    │
    ▼
OpenLineage events
```

**Best for:** When IGC is not available but you want automated dataset extraction without manual `.dsx` exports.

---

## Decision Matrix

| Criterion | Option A (dsjob+dsx) | Option B (DSODB+dsx) | Option C (DSODB+IGC) | Option D (DSODB+istool) |
|---|---|---|---|---|
| **Run history** | Last run only | All runs | All runs | All runs |
| **Never miss a run** | No | Yes | Yes | Yes |
| **Dataset names** | Yes (.dsx) | Yes (.dsx) | Yes (automatic) | Yes (automatic) |
| **Column lineage** | No | No | **Yes** | No |
| **Row counts** | Per stage | Per stage+link | Per stage+link | Per stage+link |
| **Manual steps** | Export .dsx | Export .dsx | None | None |
| **Deployment** | On DS Engine | Any machine (JDBC) | Any machine (JDBC+HTTP) | On IS server |
| **Prerequisites** | dsjob + Java | DSODB enabled + Java | DSODB + IGC + Java | DSODB + istool + Java |
| **Complexity** | Low | Low-Medium | Medium | Medium |
| **Currently implemented** | **Yes** | Not yet | Not yet | Not yet |

---

## What Is Currently Implemented

### Module: `datastage-dsjob-openlineage` (Option A)

A standalone Spring Boot application that polls `dsjob` and parses `.dsx` files.

#### Architecture

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
                              └──────────────────┘
```

#### Polling State Machine

Each job run (identified by `project:jobName:waveNumber`):

```
                               dsjob -jobinfo
                               returns status
                                     │
                                     ▼
    ┌─────────┐              ┌───────────┐
    │ Queued  │              │   NONE    │ (first time we see this wave)
    │ (13)    │──── skip ──►│           │
    └─────────┘              └─────┬─────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
        Running (0)         Completed (1,2)       Failed (3,9)
              │                    │                    │
              ▼                    ▼                    ▼
        emit START           emit START            emit START
              │              emit COMPLETE          emit FAIL
              ▼                    │                    │
         [STARTED]                 ▼                    ▼
              │               [TERMINAL]            [TERMINAL]
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

Runs in TERMINAL state are evicted after 2 hours (configurable).

#### What We Capture vs Cannot Capture

| Data point | Captured | Source |
|---|---|---|
| Job name | Yes | `dsjob -ljobs` |
| Run status (start/running/complete/fail) | Yes | `dsjob -jobinfo` → status code |
| Run identity | Yes | `dsjob -jobinfo` → wave number |
| Start/end timestamps | Yes | `dsjob -jobinfo` |
| Row counts per stage | Yes | `dsjob -report` |
| Source table names | Yes (with .dsx) | `.dsx` file parsing |
| Target table names | Yes (with .dsx) | `.dsx` file parsing |
| Connection details (host:port/db) | Yes (with .dsx) | `.dsx` file parsing |
| Full run history | **No** | Only last run visible |
| Column-level lineage | **No** | Not available via dsjob |
| Runs completed between polls | **Partial** | We see the result but not exact start time |

#### Example OpenLineage Events

**START event:**

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

**COMPLETE event (with row counts):**

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-03-15T10:35:12Z",
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
          "rowCount": 14800
        }
      }
    }
  ]
}
```

#### Configuration

```yaml
datastage:
  ds-home: /opt/IBM/InformationServer/Server/DSEngine
  projects:
    - production
    - staging
  dsx-directory: /opt/datastage/exports    # optional
  polling:
    interval: PT30S
    run-retention: PT2H

openlineage:
  enabled: true
  namespace: datastage
  transport: http
  http:
    url: http://lineage-backend:5000/api/v1/lineage
    api-key: ${OL_API_KEY:}
```

#### Deployment

```bash
# On the DataStage Engine server
source /opt/IBM/InformationServer/Server/DSEngine/dsenv

java -jar datastage-dsjob-openlineage-0.1.0-SNAPSHOT.jar \
  --datastage.projects=production,staging \
  --datastage.dsx-directory=/opt/datastage/exports \
  --openlineage.transport=http \
  --openlineage.http.url=http://lineage-backend:5000/api/v1/lineage
```

---

### Module: `datastage-openlineage` (Cloud Pak for Data)

For DataStage on IBM Cloud Pak for Data (SaaS or on-prem CPD). Uses REST API + IAM auth.

**Not relevant for DataStage 11.7 on-premise.** Included for CPD migrations.

| Aspect | dsjob module (11.7) | CPD module |
|---|---|---|
| Interface | `dsjob` CLI | REST API (HTTP) |
| Authentication | None (local) | IBM Cloud IAM token |
| Dataset resolution | `.dsx` parsing | Flow definition API (automatic) |
| Deployment | On DS Engine machine | Anywhere with network |

---

## What Is NOT Yet Implemented (Roadmap)

### Next: DSODB SQL Poller (Option B)

Would replace `dsjob` CLI polling with JDBC queries against the Operational Metadata database. This is the **recommended next step** since DSODB is already enabled.

**Architecture:**

```
┌─────────────────────┐     ┌─────────────────────────────┐
│  DSODB Database     │     │  datastage-dsodb-openlineage │
│  (DB2/Oracle/MSSQL) │◄───│  (Java app, any machine)     │
│                     │JDBC │                              │
│  JOBEXEC            │     │  DsodbPoller                 │
│  STAGERESULT        │     │  DsxParser (or IGC)          │
│  LINKRESULT         │     │  DataStageEventEmitter       │
└─────────────────────┘     └──────────┬──────────────────┘
                                       │
                                       ▼
                              OpenLineage Backend
```

**Advantages over dsjob:**
- See ALL runs (not just the last one)
- Never miss a run (timestamp-based queries)
- Run from any machine (not tied to DS Engine server)
- Structured SQL (no text parsing)
- Link-level row counts

### Later: IGC Integration (Option C)

Would add automatic dataset resolution and column-level lineage without `.dsx` exports.

### Later: BusinessObjects Integration

A separate module for SAP BusinessObjects BI reporting lineage (report → universe → database tables).

---

## Questions for the Meeting

### Infrastructure (must answer)

| # | Question | Why we need it |
|---|---|---|
| 1 | **Where is DataStage Engine installed?** (`$DSHOME` path) | To locate `dsjob` CLI |
| 2 | **What are the project names?** | To configure the poller |
| 3 | **How many jobs per project?** | Estimate poll frequency (100+ jobs → adjust interval) |
| 4 | **Can we install Java 17 on the server?** | Required for the Spring Boot poller |

### Operational Metadata (confirmed enabled)

| # | Question | Why we need it |
|---|---|---|
| 5 | **What database hosts DSODB?** (DB2, Oracle, SQL Server) | For JDBC driver and connection string |
| 6 | **Can we get read-only JDBC access to DSODB?** | To implement the DSODB poller |
| 7 | **What is the DSODB schema name?** (default: `DSODB`) | Table prefix for queries |

### Dataset Resolution (choose one path)

| # | Question | Why we need it |
|---|---|---|
| 8 | **Can the team export .dsx files for all jobs?** | For dataset name extraction (Option A/B) |
| 9 | **Is IGC installed?** | If yes, we get automatic dataset + column lineage (Option C) |
| 10 | **If IGC exists, has it scanned the DataStage projects?** | IGC must discover assets before we can query lineage |
| 11 | **Is `istool` available on the server?** | For automated .dsx/.isx export (Option D) |

### Connectivity

| # | Question | Why we need it |
|---|---|---|
| 12 | **What is the target OpenLineage backend?** (Marquez, Atlan, Purview, etc.) | HTTP transport configuration |
| 13 | **Network: can the DS server reach the lineage backend?** | If not, use console transport + ship logs |

### Connectors & Jobs

| # | Question | Why we need it |
|---|---|---|
| 14 | **What connector types are used?** (DB2, Oracle, Teradata, files, etc.) | Validate our connector mapping is complete |
| 15 | **Are there Sequence jobs?** (job A → job B → job C) | We could parse these for parent-child lineage |
| 16 | **What databases are source/target?** (names, hosts) | To verify the namespaces in events match expectations |

### Future Scope

| # | Question | Why we need it |
|---|---|---|
| 17 | **Is column-level lineage a priority?** | If yes, IGC path or .dsx transformer parsing |
| 18 | **Migration plans to Cloud Pak for Data?** | The CPD module would become relevant |
| 19 | **Are there other tools?** (BusinessObjects, SSIS, Informatica) | Extend the approach to other ETL/BI tools |

---

## Project File Structure

```
datastage-dsjob-openlineage/
├── pom.xml
└── src/main/
    ├── java/io/openlineage/datastage/dsjob/
    │   ├── DataStageDsjobApplication.java          # @SpringBootApplication + @EnableScheduling
    │   ├── config/
    │   │   ├── DsjobProperties.java                # datastage.* config (dsHome, projects, dsxDir)
    │   │   ├── OpenLineageEmitterProperties.java    # openlineage.* config (transport, namespace)
    │   │   └── DsjobOpenLineageConfiguration.java   # Spring @Bean wiring
    │   ├── command/
    │   │   ├── DsjobRunner.java                     # ProcessBuilder wrapper for dsjob CLI
    │   │   ├── JobInfo.java                         # Parsed: status, wave, timestamps
    │   │   └── JobReport.java                       # Parsed: row counts per stage
    │   ├── dsx/
    │   │   ├── DsxParser.java                       # .dsx → Map<jobName, List<DatasetDescriptor>>
    │   │   ├── DatasetDescriptor.java               # Record: namespace, name, type, stageName
    │   │   └── ConnectorType.java                   # StageType → namespace scheme enum
    │   ├── polling/
    │   │   ├── DsjobPoller.java                     # @Scheduled poll loop + state machine
    │   │   ├── TrackedRun.java                      # Per-run: NONE→STARTED→RUNNING→TERMINAL
    │   │   └── RunStateTracker.java                 # ConcurrentHashMap-based tracking + eviction
    │   └── event/
    │       └── DataStageEventEmitter.java           # Builds & emits OpenLineage RunEvents
    └── resources/
        └── application.yml

datastage-openlineage/                               # CPD module (for Cloud Pak for Data)
├── pom.xml
└── src/main/
    ├── java/io/openlineage/datastage/
    │   ├── DataStageOpenLineageApplication.java
    │   ├── config/
    │   ├── client/                                  # REST client + IAM auth + response models
    │   ├── flow/                                    # Flow definition → datasets
    │   ├── polling/                                 # Same state machine as dsjob module
    │   └── event/
    └── resources/
        └── application.yml
```

## Building

```bash
# Build all modules
mvn clean install

# Build only the dsjob module (DS 11.7)
mvn clean install -pl datastage-dsjob-openlineage -am

# Build only the CPD module
mvn clean install -pl datastage-openlineage -am
```
