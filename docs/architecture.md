# Architecture detail

This document expands on the layered data plane and the orchestration control plane. It is intentionally implementation-light: it describes *what* each component does and *why*, not the client's code.

## Data plane

### Source

A managed Azure SQL database backing the client's operational web application. It is a normalised OLTP schema spanning several hundred tables across multiple subject areas (accounts and identity, company/provider registration, assessment and quality assurance, and discretionary-grant lifecycle). Analytics cannot run against it directly without competing with production traffic.

### Ingestion — DMS into the raw zone

AWS DMS replicates the source tables into an S3 raw zone, written as parquet. Before each run the previous raw data is archived, so a run starts from a clean raw slate and the prior state is recoverable. After replication, a Glue crawler catalogs the raw zone into the Glue Data Catalog, which makes the freshly-landed tables discoverable and queryable by the transformation jobs without hand-maintained table definitions.

### Layer 1 — curated

A PySpark Glue job reads the cataloged raw tables and produces a cleaned **layer 1** in S3 (parquet). The notable cleaning behaviour:

- **Drops document/file-path columns.** Many source columns hold references to uploaded files (certificates, supporting documents, proofs of payment). These carry no analytical value and would leak storage references into the warehouse. The job detects them with a combined heuristic — column-name signals plus a scan of how many non-empty values look like file paths or URLs — and drops columns that cross a threshold. The detection is done in a **single aggregation pass** over each table rather than column-by-column scans, which keeps the job efficient on wide tables.
- **Skips operational/system tables** (sessions, migrations, permission tables, background-task bookkeeping) that have no place in an analytical model.

### Layer 2 — dimensions (Iceberg, SCD Type 2)

A nightly job builds and maintains dimension tables as Apache Iceberg tables in the Glue catalog. It uses an Iceberg `MERGE`-based SCD Type 2 strategy:

- **Close changed rows** — when a business attribute changes, the current version is expired (`effective_to` set, `current_flag` cleared).
- **Close deletions** — rows no longer present in the source are expired rather than hard-deleted, preserving history.
- **Insert new and changed** — new natural keys and changed versions are inserted as the new current rows.

Iceberg's ACID writes and snapshot isolation are what make this safe to run unattended every night: a `MERGE` either fully applies or does not, so a mid-run failure cannot leave a dimension half-updated.

### Layer 2 — facts (Iceberg, Kimball)

Fact tables are built with `CREATE TABLE AS SELECT` against the current dimension versions. They:

- resolve foreign keys to dimension **surrogate keys** (`*_sk`),
- map dates to a conformed `dim_date` and statuses to a conformed `dim_status`,
- and are partitioned/written in parquet under Iceberg.

Multiple fact subject areas are built (for example: grant-window, intervention, vetting, appeal, learning-programme, disbursement, plus assessment/certification facts), all sharing the conformed dimensions.

### Serving — PostgreSQL mirror

Publishing jobs mirror the Iceberg dimensions and facts into RDS PostgreSQL. Two details worth calling out:

- **Constraints are rebuilt.** Dimensions are published first, then facts with primary keys and foreign keys re-created against them, so the serving schema is a referentially-intact star — not just a set of disconnected tables.
- **Identifier safety.** Source identifiers can exceed Postgres's 63-character limit or collide with reserved keywords. The publish step sanitises every identifier deterministically — lowercasing, replacing invalid characters, suffixing reserved words, and applying a stable hash suffix when truncation is needed — so the same source name always maps to the same Postgres name across runs.
- **Staging then upsert.** Writes go through UNLOGGED staging tables that are dropped after each upsert, which keeps the publish step fast and avoids bloating the WAL for what is effectively a bulk reload.

## Control plane

### Outer state machine — ingestion

Responsibilities, in order: archive previous raw data → start DMS → poll DMS status → run raw crawler → poll crawler → **validate raw data** → invoke the inner pipeline synchronously.

The **validate raw** step is a deliberate quality gate: the transformation jobs do not run unless the freshly-landed raw data passes validation. This prevents a bad or partial replication from silently propagating into the warehouse.

### Inner state machine — transformation

Runs the Glue jobs in strict sequence: clean to layer 1 → build SCD2 dimensions → build facts → publish dimensions and facts to Postgres. Each job is started and polled through a `start → wait → check → choice` loop.

### Reliability patterns

- **Polling loops** for every asynchronous AWS resource (DMS, crawlers, Glue jobs), because these complete on their own schedule and the state machine must wait without burning a long-running task.
- **Exponential backoff with jitter** on transient errors, with bounded retry counts.
- **Synchronous nesting** (`startExecution.sync`) so the outer execution's success/failure reflects the real end-to-end outcome.
- **SNS failure notification** wired from failed states in both machines, so failures are surfaced operationally instead of being discovered as stale dashboards the next morning.
