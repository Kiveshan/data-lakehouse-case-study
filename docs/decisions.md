# Design decisions

Short ADR-style records of the decisions that shaped this system. Each notes the context, the choice, and the trade-off — including the ones I would revisit.

## 1. Medallion (multi-hop) layering rather than a single transform

**Context.** The source is a few hundred normalised OLTP tables; the target is an analytical star schema.

**Decision.** Refine in discrete, materialised stages — raw → curated layer 1 → modelled layer 2 — each persisted to S3.

**Why.** Each stage is independently inspectable and re-runnable. When a fact looks wrong, I can check whether the issue is in raw landing, cleaning, or modelling without re-running the whole chain. Materialising intermediate layers costs storage but storage is cheap; debugging a monolithic transform is not.

**Trade-off.** More storage and more moving parts than a single ELT pass. Acceptable for a nightly batch system; would reconsider for low-latency requirements.

## 2. Apache Iceberg for the warehouse layer

**Context.** SCD Type 2 dimensions are updated every night with `MERGE`; partial writes would corrupt history.

**Decision.** Use Apache Iceberg (format v2) tables in the Glue catalog for dimensions and facts, rather than plain parquet directories.

**Why.** Iceberg provides ACID writes, snapshot isolation, and schema evolution. The nightly `MERGE` is atomic, so a failure mid-run cannot leave a dimension half-updated. Schema evolution matters because the source schema changes over time and the warehouse must absorb that without a full rebuild.

**Trade-off.** Iceberg adds operational concepts (snapshots, metadata, table maintenance) over raw parquet. Worth it the moment you need safe in-place updates.

## 3. SCD Type 2 history in dimensions

**Context.** Reporting needs point-in-time accuracy — what was the company's status / sector / classification *at the time* of an event, not today.

**Decision.** Every dimension carries surrogate keys and SCD Type 2 versioning (`effective_from`, `effective_to`, `current_flag`); deletions are expired, not removed.

**Why.** Without it, restating a dimension would silently rewrite history in every fact that joins to it. SCD2 preserves the truth as of each event.

**Trade-off.** Dimensions grow over time and queries must filter `current_flag` (or join on the version valid at the event date). More complex than overwrite (Type 1), but the analytical requirement demanded it.

## 4. Conformed `dim_date` and `dim_status`

**Decision.** A single shared date dimension and a single shared status dimension, referenced by every fact.

**Why.** Conformed dimensions are what make cross-subject-area reporting consistent — the same date logic and the same status vocabulary everywhere, resolved once.

**Trade-off.** Requires discipline in the fact builds to always map through the conformed dimensions rather than carrying raw dates/statuses. That discipline is the point.

## 5. Two nested Step Functions state machines

**Context.** Ingestion (DMS, crawling) and transformation (Glue jobs) have different failure modes and timescales.

**Decision.** An outer machine owns ingestion and a quality gate; it invokes an inner machine that owns transformation, synchronously.

**Why.** Each state machine stays small and independently testable. The transformation pipeline can be re-run on its own when ingestion already succeeded. Failure blast radius is isolated. The synchronous invoke means the outer execution's status is the true end-to-end status.

**Trade-off.** Two definitions to maintain instead of one. The separation pays for itself the first time you need to re-run only the transform half.

## 6. A data-quality gate before transformation

**Decision.** A `validate raw` step must pass before any transformation job runs.

**Why.** A partial or failed DMS replication should never silently flow into the warehouse and produce confident-looking but wrong dashboards. Failing loudly at the gate is far cheaper than discovering corrupted facts downstream.

**Trade-off.** Adds a step that can block a run. That is the desired behaviour.

## 7. PostgreSQL serving layer mirrored from Iceberg

**Context.** The consuming application already uses SQL against Postgres.

**Decision.** Mirror the modelled star schema into RDS PostgreSQL, rebuilding PK/FK constraints, rather than pointing the app at an Iceberg query engine.

**Why.** Gives the application a familiar, low-latency, referentially-intact target, and decouples it from the lake's query engine and its operational characteristics.

**Trade-off.** A copy step and two stores to keep in sync. Justified by the consumer's existing Postgres dependency and latency needs.

## 8. Polling Lambdas for async resource status — *would revisit*

**Context.** DMS, crawlers, and Glue jobs complete asynchronously and must be waited on.

**Decision (as built).** Lambda task workers start each resource and poll its status, driving Step Functions choice loops.

**Why it was done this way.** Full control over status parsing and the ability to normalise heterogeneous status shapes into a single decision variable.

**What I would change.** Step Functions offers native `.sync` integrations for Glue jobs and SDK integrations for DMS and crawlers that eliminate most of this polling and the associated Lambda cold-starts. Migrating to native integrations would cut moving parts and operational surface. This is the clearest improvement on the list.
