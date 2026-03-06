# Spring Batch OpenLineage

Automatic [OpenLineage](https://openlineage.io) integration for Spring Batch.

Spring Batch is widely used for ETL, data migrations, and scheduled data processing. But batch jobs are often black boxes — data flows in, transformations happen, data flows out, and nobody has a clear picture of what reads what, what writes where, or how jobs relate to each other.

OpenLineage solves this by defining a standard for data lineage events. This library bridges the gap: it automatically instruments your Spring Batch jobs to emit OpenLineage events, giving you full visibility into your batch data pipelines without changing a single line of job code.

## Project structure

```
spring-batch-openlineage/          # Spring Batch auto-instrumentation library
spring-batch-openlineage-demo/     # Demo application for the library
datastage-openlineage/             # DataStage poller for Cloud Pak for Data (SaaS/CPD)
datastage-dsjob-openlineage/       # DataStage poller for on-premise 11.7 via dsjob CLI
```

---

## Module 1: spring-batch-openlineage

### What it does

Drop the library into a Spring Boot application that uses Spring Batch, and it will:

- **Track job lifecycle** — emit START and COMPLETE/FAIL events for every job execution
- **Track step lifecycle** — emit START and COMPLETE/FAIL events for every step, with parent-child links back to the job
- **Track chunk progress** — emit RUNNING events as chunks are processed, with live write counts
- **Extract dataset metadata** — automatically detect input and output datasets from your readers and writers (JDBC tables, CSV files, etc.)
- **Enrich with metrics** — attach row counts and execution statistics to output datasets

All of this happens transparently through Spring Boot auto-configuration. No annotations, no manual listener wiring, no code changes to your jobs.

### Supported readers and writers

| Component | Detected as | Extracted metadata |
|---|---|---|
| `JdbcCursorItemReader` | Input dataset | JDBC URL + table name (parsed from SQL) |
| `JdbcPagingItemReader` | Input dataset | JDBC URL + table name (parsed from SQL) |
| `JdbcBatchItemWriter` | Output dataset | JDBC URL + table name (parsed from SQL) |
| `FlatFileItemReader` | Input dataset | File path (classpath, filesystem, or URL) |
| `FlatFileItemWriter` | Output dataset | File path (classpath, filesystem, or URL) |
| Tasklet steps | (no datasets) | Gracefully handled — events emitted, datasets empty |
| Other reader/writer types | Fallback | Detected with generic "unknown" namespace |

The extractor system is pluggable — implement `DatasetExtractor` to add support for additional reader/writer types.

### Quick start

#### 1. Add the dependency

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>spring-batch-openlineage</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

#### 2. Configure

```yaml
openlineage:
  enabled: true
  namespace: my-batch-app
  transport: http
  http:
    url: http://localhost:5000   # Your OpenLineage-compatible backend
```

That's it. Every Spring Batch job in your application will now emit lineage events.

#### 3. Configuration reference

| Property | Default | Description |
|---|---|---|
| `openlineage.enabled` | `true` | Master switch |
| `openlineage.namespace` | `spring-batch` | Namespace for all jobs |
| `openlineage.transport` | `console` | Transport type: `http`, `console`, or `noop` |
| `openlineage.http.url` | — | HTTP endpoint URL |
| `openlineage.http.api-key` | — | Optional API key for authentication |
| `openlineage.granularity.job-events` | `true` | Emit job-level START/COMPLETE events |
| `openlineage.granularity.step-events` | `true` | Emit step-level START/COMPLETE events |
| `openlineage.granularity.chunk-events` | `true` | Emit chunk-level RUNNING events |
| `openlineage.granularity.chunk-event-interval` | `1` | Emit every Nth chunk (use higher values to reduce noise) |

---

## Module 2: spring-batch-openlineage-demo

Demo application running two jobs against an in-memory H2 database.

### Job 1: `importPeopleJob`

```
people.csv  --[csvToDatabaseStep]--> people (DB)
```

### Job 2: `analyticsJob`

```
people (DB) --[enrichStep]--> enriched_people (DB)
enriched_people (DB) --[exportSeniorsStep]--> output/seniors.csv
enriched_people (DB) --[exportJuniorsStep]--> output/juniors.csv
enriched_people (DB) --[statsStep]--> (logs summary)
```

### Running the demo

```bash
mvn clean install
cd spring-batch-openlineage-demo
mvn spring-boot:run
```

---

## Module 3: datastage-openlineage

Standalone Spring Boot poller for **IBM DataStage on Cloud Pak for Data** (SaaS or on-prem CPD). Polls the DataStage REST API every 30 seconds and emits standard OpenLineage events.

### Architecture

```
IBM DataStage (Cloud Pak for Data)
       |
       | REST API (polled every 30s)
       v
┌─────────────────────────────────┐
│  IamTokenManager                │  ← auto-refreshes IAM bearer token
│  DataStageClient                │  ← wraps all REST calls
│  FlowParser                     │  ← extracts datasets from flow definitions
│  DataStagePoller (@Scheduled)   │  ← state machine per run
│  DataStageEventEmitter          │  ← builds & emits OL RunEvents
└─────────────────────────────────┘
       |
       v
   Any OpenLineage backend
```

### Configuration

```yaml
datastage:
  api-key: ${DATASTAGE_API_KEY}
  service-url: https://api.dataplatform.cloud.ibm.com
  project-id: ${DATASTAGE_PROJECT_ID}
  polling:
    interval: PT30S
    run-retention: PT2H

openlineage:
  namespace: datastage
  transport: console   # or http
  http:
    url: http://localhost:5000
```

### Running

```bash
DATASTAGE_API_KEY=your-key DATASTAGE_PROJECT_ID=your-project \
  mvn -pl datastage-openlineage spring-boot:run
```

---

## Module 4: datastage-dsjob-openlineage

Standalone Spring Boot poller for **IBM DataStage 11.7 on-premise** (legacy Server). Uses the `dsjob` CLI instead of REST APIs. Must run on the same machine as the DataStage Engine.

### Architecture

```
DataStage 11.7 Engine (same machine)
       |
       | dsjob CLI (polled every 30s)
       v
┌──────────────────────────────────┐
│  DsjobRunner                     │  ← ProcessBuilder wrapper for dsjob
│  DsxParser                       │  ← parses .dsx exports for dataset names
│  DsjobPoller (@Scheduled)        │  ← state machine per job/wave
│  DataStageEventEmitter           │  ← builds & emits OL RunEvents
└──────────────────────────────────┘
       |
       v
   Any OpenLineage backend
```

### How it works

1. **Poll loop** — every 30s, calls `dsjob -ljobs` to list jobs, then `dsjob -jobinfo` for each to get status and wave number
2. **State machine** — tracks each run by `project:jobName:waveNumber`, emitting START → RUNNING → COMPLETE/FAIL events
3. **Row counts** — calls `dsjob -report` to get per-stage row counts, attached as OutputStatistics facets
4. **Dataset names** — if `.dsx` export files are provided, parses them to extract real table names, file paths, and connection details (namespace = `db2://host:port/database`, name = `CUSTOMERS`)

### dsjob status code mapping

| Code | Status | OpenLineage event |
|---|---|---|
| 0 | Running | START (first seen) / RUNNING (subsequent) |
| 1 | RUN OK | COMPLETE |
| 2 | RUN with warnings | COMPLETE |
| 3 | RUN FAILED | FAIL |
| 9 | Stopped | FAIL |
| 13 | Queued | (wait) |
| Other | Reset/Validated/etc | (skip) |

### DSX parser for dataset extraction

Without `.dsx` files, events use stage names as dataset identifiers. With `.dsx` files, you get real dataset metadata:

| Without DSX | With DSX |
|---|---|
| namespace: `datastage` | namespace: `db2://dbserver:50000/warehouse` |
| name: `project.job.Target_Stage` | name: `CUSTOMERS` |

The parser extracts from each connector stage:
- **Connector type** → namespace scheme (`DB2ConnectorPX` → `db2://`, `OracleConnectorPX` → `oracle://`, etc.)
- **Table name or file path** → dataset name
- **Server/port/database** → full namespace
- **InputPins/OutputPins** → INPUT (source) or OUTPUT (target)

Supported connector types: DB2, Oracle, PostgreSQL, MySQL, SQL Server, Teradata, Netezza, ODBC, Sequential File, Dataset (both parallel PX and server stage variants).

To use: export your DataStage jobs as `.dsx` files from the Designer, place them in a directory, and set `datastage.dsx-directory`.

### Configuration

```yaml
datastage:
  ds-home: ${DSHOME:/opt/IBM/InformationServer/Server/DSEngine}
  projects:
    - project1
    - project2
  dsx-directory: /opt/datastage/exports   # optional, for real dataset names
  polling:
    interval: PT30S
    run-retention: PT2H

openlineage:
  namespace: datastage
  transport: console   # or http
  http:
    url: http://localhost:5000
```

| Property | Default | Description |
|---|---|---|
| `datastage.ds-home` | `/opt/IBM/InformationServer/Server/DSEngine` | Path to DSEngine installation |
| `datastage.projects` | — | List of DataStage projects to poll |
| `datastage.dsx-directory` | — | Directory containing `.dsx` export files (optional) |
| `datastage.polling.interval` | `PT30S` | Poll interval |
| `datastage.polling.run-retention` | `PT2H` | How long to keep terminal runs before eviction |

### Prerequisites

- Java 17+ on the DataStage Engine machine
- `dsjob` accessible (source `dsenv` before running)
- The Spring Boot app runs on the same machine as DSEngine

### Running

```bash
# Source the DataStage environment
source /opt/IBM/InformationServer/Server/DSEngine/dsenv

# Run the poller
java -jar datastage-dsjob-openlineage-0.1.0-SNAPSHOT.jar \
  --datastage.projects=project1,project2

# Or with DSX files for real dataset names
java -jar datastage-dsjob-openlineage-0.1.0-SNAPSHOT.jar \
  --datastage.projects=project1,project2 \
  --datastage.dsx-directory=/opt/datastage/exports
```

### Polling state machine

```
         first seen as Queued
                  │
                  v
               [NONE] ── first seen as Running ──> emit START ──> [STARTED]
                  │                                                    │
                  │                                     poll again ──> emit RUNNING ──> [RUNNING]
                  │                                                    │
                  │                                     terminal ───> emit COMPLETE/FAIL ──> [TERMINAL]
                  │
                  └── first seen as Completed ──> emit START + COMPLETE ──> [TERMINAL]
                  └── first seen as Failed ─────> emit START + FAIL ──────> [TERMINAL]
```

Runs in TERMINAL state are evicted after `run-retention` (default 2 hours).

---

## Compatible backends

All modules emit standard OpenLineage events. They work with any backend that implements the OpenLineage API:

- [Marquez](https://marquezproject.ai)
- [Atlan](https://atlan.com)
- [Google Cloud Data Catalog / Dataplex](https://cloud.google.com/dataplex/docs/open-lineage)
- [Microsoft Purview](https://learn.microsoft.com/en-us/purview/concept-best-practices-lineage-azure-data-factory#openlineage-support)
- [Egeria](https://egeria-project.org)
- Any custom service accepting OpenLineage HTTP events

Use `transport: console` during development to see events logged to stdout without needing a backend.

## How it works (Spring Batch module)

The library uses Spring Boot auto-configuration and a `BeanPostProcessor` to transparently instrument your batch jobs:

1. **Auto-configuration** (`OpenLineageAutoConfiguration`) activates when Spring Batch and the OpenLineage client are both on the classpath
2. **`OpenLineageBatchConfigurer`** (a `BeanPostProcessor`) intercepts every `Job` and `Step` bean as Spring creates them, and registers the appropriate listeners
3. For chunk-oriented steps, it uses reflection to extract the `ItemReader` and `ItemWriter`, then passes them to **dataset extractors** that resolve input/output dataset metadata
4. At runtime, the **listeners** emit OpenLineage events at each lifecycle point (job start/complete, step start/complete, chunk complete)
5. Events are sent through the configured **transport** (HTTP, console, or noop)

No `@EnableOpenLineage` annotation, no manual listener registration — just add the dependency and configure your backend.

## Requirements

- Java 17+
- Spring Boot 3.x
- Spring Batch 5.x (for the spring-batch-openlineage module)
- DataStage 11.7 + `dsjob` CLI (for the datastage-dsjob-openlineage module)
- DataStage on Cloud Pak for Data (for the datastage-openlineage module)

## Building

```bash
mvn clean install   # builds all 4 modules
```
