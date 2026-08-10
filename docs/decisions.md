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

## 9. AWS Glue (Spark) as the transformation engine, not Lambda

**Context.** The nightly transform does distributed joins across several hundred tables, an Iceberg `MERGE` maintaining SCD Type 2 dimensions, and `CREATE TABLE AS SELECT` fact builds resolving surrogate keys — roughly 750,000 records per run.

**Decision.** Run all transformation on AWS Glue PySpark jobs. Keep Lambda in the architecture, but only as control-plane task workers that start and poll DMS, crawlers, and Glue jobs.

**Why.** Three independent disqualifiers for Lambda as the transform engine:

- *Compute model.* Lambda is single-node with a 10 GB memory ceiling. The work is a Spark workload; doing it on Lambda means hand-sharding across invocations and reimplementing a shuffle and a scheduler.
- *Execution ceiling.* Lambda hard-stops at 15 minutes. The failure mode is not a slow job but a truncated one — a timeout part-way through a dimension `MERGE` is precisely the corruption decision 2 exists to prevent.
- *Ecosystem fit.* Glue reads the Data Catalog the crawler populates, ships the Iceberg runtime, and writes parquet to S3 natively. On Lambda every one of those is a dependency-packaging problem.

**Trade-off.** Glue has a job startup cost of a minute or two and bills per DPU-hour with a minimum billing duration, which makes it genuinely wasteful for small, fast operations. That is exactly why the sub-second, stateless "start this job / what is its status" calls stayed on Lambda. The decision was never Glue *or* Lambda — it was distributed transforms on Glue, control-plane calls on Lambda.

**Alternatives considered.** EMR — more engine control and better economics at sustained scale, but adds cluster lifecycle management to a client with no data team. Glue Python Shell — avoids Spark startup cost, but returns to single-node compute.

## 10. Step Functions for orchestration, not Airflow/MWAA

**Context.** One pipeline, one run per night, a client with no data engineering team to operate the orchestrator.

**Decision.** AWS Step Functions state machines triggered by EventBridge Scheduler.

**Why.**

- *Nothing to keep running.* MWAA bills for an always-on environment regardless of DAG activity. Step Functions bills per state transition, which at one run per night is negligible, and EventBridge Scheduler removes the scheduler process entirely.
- *Retries are configuration.* Exponential backoff, jitter, bounded retry counts, and `Catch` routing to SNS are declared in the state machine definition rather than written and tested as callback code.
- *Execution history is the audit trail.* Per-run inputs, outputs, and per-state timings are retained and inspectable with no logging infrastructure or metadata database to operate. For a compliance-adjacent public-sector workload that is a genuine requirement, not a convenience.
- *IAM boundaries per state,* rather than a worker fleet holding broad credentials.

**Trade-off.** Amazon States Language is verbose and hard to test locally — there is no clean "run the DAG on your laptop" story, so iteration means deploying. Airflow's Python authoring, backfill semantics, and dependency graph are better *if you already operate Airflow*. Standard workflows also cap execution history events, which would constrain a per-table fan-out design; the fixed-sequence design here stays well below the limit.

**Alternatives considered.** A cron-triggered Lambda chaining each job on completion — cheapest possible, rejected because failure handling, partial re-runs, and "where did last night's run stop?" all become bespoke code with no state model behind them.

## 11. AWS DMS for extraction, not a Glue JDBC read

**Context.** The source is a live Azure Database for MySQL backing a production web application. The premise of the project is that analytics must not degrade it.

**Decision.** Use AWS DMS to replicate source tables into the S3 raw zone as parquet.

**Why.** DMS is purpose-built for reading operational databases without holding long-running transactions against them, and it lands parquet in S3 with no bespoke connector code and no extraction logic to maintain as the source schema drifts. A Glue JDBC extract would have meant writing and maintaining per-table read logic and reasoning about its transaction footprint on production myself.

**Trade-off.** A replication instance costs money whether or not it is replicating, and the current configuration does a nightly full load rather than true CDC. Moving to CDC is the obvious lever if source volume grows materially.

## 12. Nightly batch, not streaming or micro-batch

**Context.** The analytical requirements are historical trend analysis, compliance reporting, and sector-wide aggregation.

**Decision.** A single scheduled nightly run.

**Why.** None of the required reporting improves with sub-daily freshness. Streaming would multiply cost, operational surface, and failure modes for a benefit no user would observe.

**Trade-off.** Up to 24 hours of staleness. This is an accepted and documented property of the system rather than an oversight — and it is the assumption to revisit first if the client ever asks for intraday reporting.

## 13. Archive-then-reload the raw zone each run

**Decision.** Archive the previous raw data before each DMS run, so every run starts from a clean raw slate with the prior state recoverable.

**Why.** Idempotency and a simple mental model: a run either produced a complete raw zone or it did not, and last night's landing is still on disk when you need to work out what changed. Combined with the quality gate (decision 6), a bad replication is contained rather than blended into good data.

**Trade-off.** Storage grows with retention, and full reload cost scales with source volume rather than with change volume. At current size that cost is trivial; at ten times the volume this is the first thing to revisit alongside DMS CDC.

## 14. `af-south-1` as the deployment region

**Context.** South African public-sector data, POPIA obligations, source system already in-country.

**Decision.** Deploy the entire lakehouse in `af-south-1` (Cape Town).

**Why.** Keeping the analytical copy in-country removes the cross-border transfer question entirely and keeps latency to the client's users low.

**Trade-off.** A real one: `af-south-1` has a smaller service catalogue, higher unit pricing than `us-east-1`, and new AWS features arrive late. Accepted, because data residency is not something to trade for a discount.

## 15. SNS notification rather than a monitoring stack

**Decision.** Wire SNS failure notification from failed states in both machines, and stop there.

**Why.** The operational requirement is that a failed run must not be discovered as a stale dashboard the next morning. An SNS alert meets that requirement completely. CloudWatch dashboards, anomaly detection, or a paging tool would be more capable and would also be infrastructure nobody at the client is positioned to maintain.

**Trade-off.** No trend visibility on run duration or record counts, so gradual degradation is invisible until it becomes a failure. Right-sizing the operational surface to the team that inherits the system is itself the decision; emitting run metrics to CloudWatch would be a cheap, proportionate next step.

## 16. Provisioned via console and SDK, not IaC — *revisited*

**Context.** The system grew incrementally against a live client need rather than being designed up-front.

**Decision (as built).** Resources were created through the AWS console and SDK as the pipeline took shape.

**Why it was done this way.** It was the fastest path to a working nightly pipeline while the shape of the solution was still moving, and the client's need was for a running system, not a reproducible one.

**What I changed.** I rebuilt the whole design as Terraform — [datalakehouse-infra](https://github.com/Kiveshan/datalakehouse-infra): remote S3-backed state with native locking, the DMS wiring extracted into a self-contained, account-agnostic module, and both Step Functions state machines templated from the actual Lambda ARNs and Glue job names so the definitions can't drift from what's deployed. It captures what a reproducible, reviewable version of this environment should look like — plan-on-PR is the natural next step once it has a CI story.

**What's still open.** This Terraform targets a fresh AWS account, not the client's live one — it's a from-scratch reconstruction of the design, not a cutover of the production system this case study describes. It hasn't been run against real data yet, so at least one piece (`build_etqa_facts`'s join logic) is a documented, unverified placeholder pending a real crawl. Cutting the actual client environment over to this Terraform — importing existing resources, reconciling state, verifying zero downtime — is separate, unstarted work.
