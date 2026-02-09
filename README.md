# Spring Batch OpenLineage

Automatic [OpenLineage](https://openlineage.io) integration for Spring Batch.

Spring Batch is widely used for ETL, data migrations, and scheduled data processing. But batch jobs are often black boxes — data flows in, transformations happen, data flows out, and nobody has a clear picture of what reads what, what writes where, or how jobs relate to each other.

OpenLineage solves this by defining a standard for data lineage events. This library bridges the gap: it automatically instruments your Spring Batch jobs to emit OpenLineage events, giving you full visibility into your batch data pipelines without changing a single line of job code.

## What it does

Drop the library into a Spring Boot application that uses Spring Batch, and it will:

- **Track job lifecycle** — emit START and COMPLETE/FAIL events for every job execution
- **Track step lifecycle** — emit START and COMPLETE/FAIL events for every step, with parent-child links back to the job
- **Track chunk progress** — emit RUNNING events as chunks are processed, with live write counts
- **Extract dataset metadata** — automatically detect input and output datasets from your readers and writers (JDBC tables, CSV files, etc.)
- **Enrich with metrics** — attach row counts and execution statistics to output datasets

All of this happens transparently through Spring Boot auto-configuration. No annotations, no manual listener wiring, no code changes to your jobs.

## Supported readers and writers

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

## Project structure

```
spring-batch-openlineage/        # The library (auto-configuration + listeners + extractors)
spring-batch-openlineage-demo/   # Demo application showing the library in action
```

## Quick start

### 1. Add the dependency

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>spring-batch-openlineage</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure

```yaml
openlineage:
  enabled: true
  namespace: my-batch-app
  transport: http
  http:
    url: http://localhost:5000   # Your OpenLineage-compatible backend
```

That's it. Every Spring Batch job in your application will now emit lineage events.

### 3. Configuration reference

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

## Compatible backends

The library emits standard OpenLineage events over HTTP. It works with any backend that implements the OpenLineage API:

- [Marquez](https://marquezproject.ai)
- [Atlan](https://atlan.com)
- [Google Cloud Data Catalog / Dataplex](https://cloud.google.com/dataplex/docs/open-lineage)
- [Microsoft Purview](https://learn.microsoft.com/en-us/purview/concept-best-practices-lineage-azure-data-factory#openlineage-support)
- [Egeria](https://egeria-project.org)
- Any custom service accepting OpenLineage HTTP events

Use `transport: console` during development to see events logged to stdout without needing a backend.

## Demo application

The demo module (`spring-batch-openlineage-demo`) runs two jobs against an in-memory H2 database to showcase the library's capabilities.

### Job 1: `importPeopleJob`

Imports 50 people from a CSV file into a database table.

```
people.csv  --[csvToDatabaseStep]--> people (DB)
```

- Reader: `FlatFileItemReader` (CSV)
- Writer: `JdbcBatchItemWriter`
- Chunk size: 10

### Job 2: `analyticsJob`

A 4-step analytics pipeline that enriches, partitions, exports, and summarizes the data:

```
people (DB) --[enrichStep]--> enriched_people (DB)
enriched_people (DB) --[exportSeniorsStep]--> output/seniors.csv
enriched_people (DB) --[exportJuniorsStep]--> output/juniors.csv
enriched_people (DB) --[statsStep]--> (logs summary)
```

| Step | Reader | Writer | Chunk size |
|---|---|---|---|
| `enrichStep` | `JdbcCursorItemReader` | `JdbcBatchItemWriter` | 5 |
| `exportSeniorsStep` | `JdbcCursorItemReader` | `FlatFileItemWriter` | 15 |
| `exportJuniorsStep` | `JdbcCursorItemReader` | `FlatFileItemWriter` | 15 |
| `statsStep` | Tasklet (no reader/writer) | — | — |

This demonstrates:
- Different reader/writer combinations (DB-to-DB, DB-to-CSV)
- Different chunk sizes producing different event counts
- A plain Tasklet step with no datasets (gracefully handled)
- Parent-child lineage across multiple jobs and steps

### Running the demo

```bash
# Build everything
mvn clean install

# Run the demo
cd spring-batch-openlineage-demo
mvn spring-boot:run
```

Both jobs run sequentially. You'll see lineage events in the logs (DEBUG level), and if you have an OpenLineage backend running at `http://localhost:8000`, events will be sent there too.

Output files are written to `spring-batch-openlineage-demo/output/`:
- `seniors.csv` — people aged 30+
- `juniors.csv` — people under 30

## How it works

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
- Spring Batch 5.x
