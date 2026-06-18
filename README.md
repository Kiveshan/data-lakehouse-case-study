# Public-sector data lakehouse — architecture case study

An end-to-end AWS data lakehouse that ingests a production operational database nightly, refines it through a medallion (multi-hop) architecture, models it into a Kimball star schema on Apache Iceberg, and serves it from PostgreSQL. Running unattended in production since August 2025, processing roughly **750,000 records per nightly run**.

**Stack:** AWS DMS · S3 · Glue (PySpark) · Apache Iceberg · Step Functions · EventBridge Scheduler · Lambda · SNS · RDS PostgreSQL · `af-south-1`

> **No source code in this repository.** This system was built for a paying client under confidentiality. This repo is an architecture write-up only — diagrams, design decisions, and trade-offs. No scripts, credentials, schemas, or client data are included. See [Why there is no code here](#why-there-is-no-code-here).

---

## Context

The client is a South African public-sector skills-development authority. Their operational platform runs on a managed Azure SQL database behind a web application. They needed analytical reporting — historical trend analysis, compliance reporting, and sector-wide aggregations — that could not be run against the live transactional database without degrading the application.

The problem was the classic one: a normalised, highly relational OLTP schema (hundreds of tables) is the wrong shape for analytics, and querying it directly competes with production traffic. The brief was to stand up a separate analytical layer, refreshed nightly, that preserves point-in-time history and presents a query-friendly model — without a dedicated data team to operate it.

## Outcome

- A lakehouse refreshed **nightly, unattended**, in production since August 2025.
- ~**750,000 records** ingested per run from the Azure SQL source.
- **~400 modelled entities** in a Kimball-compliant dimensional schema with full SCD Type 2 history.
- **Zero manual steps** in the steady state: a single scheduled trigger runs ingestion, transformation, modelling, and publishing, with automatic retries and failure alerting.
- Analytical load fully isolated from the production OLTP database.

## Architecture

The data plane is a medallion architecture: raw data lands untouched, then is progressively cleaned and modelled across discrete layers, each materialised in S3.

![Data lineage](diagrams/data-lineage.svg)

1. **Source — Azure SQL.** The live operational database, owned by the client's application.
2. **Ingestion — AWS DMS.** Database Migration Service replicates the source tables into an S3 raw zone as parquet. A Glue crawler catalogs the raw zone into the Glue Data Catalog so it is queryable and discoverable by downstream jobs.
3. **Layer 1 — curated (S3).** A PySpark Glue job cleans the raw tables: it standardises types, and removes document/file-path columns that carry no analytical value (and that would otherwise leak storage references into the warehouse). Output is parquet under a `curated/layer1/` prefix.
4. **Layer 2 — dimensions (Iceberg).** A nightly SCD Type 2 job builds and maintains dimension tables as Apache Iceberg tables, using `MERGE` to close changed rows, expire deleted rows, and insert new/changed versions — preserving point-in-time history.
5. **Layer 2 — facts (Iceberg).** Fact tables are built as a Kimball star schema using `CREATE TABLE AS SELECT`, joining to dimensions on surrogate keys and resolving a shared date dimension and status dimension.
6. **Serving — RDS PostgreSQL.** Publishing jobs mirror the Iceberg dimensions and facts into PostgreSQL, rebuilding primary and foreign key constraints so the serving layer is a fully relational, referentially-intact star schema.
7. **Consumers.** The application's reporting features and BI queries run against the Postgres star schema, never against the source OLTP database.

See [docs/architecture.md](docs/architecture.md) for the per-layer detail, including the data-quality gate and the identifier-safety handling in the publish step.

## Orchestration

The control plane is a two-level Step Functions design that separates **ingestion** from **transformation**. This separation matters: ingestion (DMS + crawling) has very different failure modes and timing characteristics from the transformation jobs, and keeping them in distinct state machines keeps each one small and independently testable.

![Orchestration](diagrams/orchestration.svg)

- **EventBridge Scheduler** fires the pipeline nightly.
- The **outer state machine** archives the previous raw data, starts DMS, polls until replication completes, runs and polls the raw crawler, then runs a **data-quality gate** before going further. It then invokes the inner machine synchronously (`startExecution.sync`) so the outer execution reflects the true end-to-end status.
- The **inner state machine** runs the Glue jobs in strict sequence: raw → layer 1 → dimensions → facts → publish-to-Postgres. Each step follows a `start → wait → check → choice` polling loop, with exponential backoff, jitter, and retry limits.
- **Lambda functions** are the task workers that start and poll Glue jobs, DMS, and crawlers.
- **SNS** sends a failure notification from any failed state, so a broken run is surfaced rather than silently producing stale data.

## Data model

The warehouse is a deliberate Kimball star schema, not a flattened dump:

- **Dimensions** carry surrogate keys (`*_sk`) and full SCD Type 2 versioning (`effective_from`, `effective_to`, `current_flag`), so a fact can be analysed against the attribute values that were true at the time of the event.
- **Conformed dimensions** — a single `dim_date` and a single `dim_status` — are shared across every fact, so reporting is consistent across subject areas.
- **Facts** reference dimensions by surrogate key and are published to Postgres with real foreign-key constraints, which both documents the model and catches referential breaks early.

## Tech stack

| Concern | Choice |
| --- | --- |
| Source extraction / CDC | AWS DMS |
| Object storage | Amazon S3 (parquet) |
| Table format (warehouse) | Apache Iceberg (format v2) |
| Transformation engine | AWS Glue (PySpark / Spark SQL) |
| Cataloging | Glue Data Catalog + Glue crawlers |
| Orchestration | AWS Step Functions (nested) |
| Scheduling | Amazon EventBridge Scheduler |
| Task workers | AWS Lambda |
| Alerting | Amazon SNS |
| Serving database | Amazon RDS for PostgreSQL |

## Key design decisions

The full set with rationale and trade-offs is in [docs/decisions.md](docs/decisions.md). The three that matter most:

- **Apache Iceberg over plain parquet for the warehouse layer.** Iceberg gives ACID writes, schema evolution, and snapshot isolation, which makes the SCD Type 2 `MERGE` pattern safe to run nightly without partial-write corruption. Plain parquet would have required hand-rolling all of that.
- **Two nested state machines instead of one.** Ingestion and transformation fail differently and at different timescales. Splitting them keeps each state machine small, makes the transformation pipeline re-runnable on its own when ingestion already succeeded, and isolates blast radius.
- **A relational Postgres serving layer rather than querying Iceberg directly.** The consuming application already speaks SQL to Postgres. Mirroring the star schema into RDS — with PK/FK constraints intact — gave the app a familiar, low-latency, referentially-safe target without coupling it to the lake's query engine.

## What I would improve

Being honest about the trade-offs is part of the point of this write-up.

- **Replace polling Lambdas with native Step Functions service integrations.** Much of the orchestration polls Glue jobs and crawlers through Lambda workers. Step Functions supports `.sync` integrations for Glue jobs and SDK integrations for DMS and crawlers, which would remove most of those Lambdas and their cold-start and status-parsing overhead.
- **Confirm and codify incremental load at layer 1.** The SCD Type 2 step is incremental; the upstream cleaning step should be audited to ensure it is not reprocessing unchanged tables in full every night, which is the obvious cost lever as the dataset grows.
- **Infrastructure as code.** This system was provisioned through the console and SDK over time. Capturing it in Terraform (with remote state and plan-on-PR) would make it reproducible and reviewable — that migration is in progress separately.

## Why there is no code here

The pipeline scripts contain client-specific schema, table mappings, connection details, and business logic that belong to the client. Publishing them would expose confidential information and infrastructure detail. A redacted code dump also reads worse to an engineer than a clear architecture write-up: the value of this project is the *design* — the layering, the history-preservation strategy, the orchestration model — and that is exactly what this repo communicates. The diagrams here are reconstructions that contain no account identifiers, endpoints, ARNs, or credentials.

## Repository structure

```
.
├── README.md                  this case study
├── docs/
│   ├── architecture.md        per-layer detail + control plane
│   └── decisions.md           design decisions with trade-offs (ADR style)
└── diagrams/
    ├── data-lineage.svg       source-to-serving data flow
    └── orchestration.svg      nested Step Functions control plane
```
