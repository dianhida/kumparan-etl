# Phase 3 — Warehouse Design & Initialization

## Objective

Design and initialize the DuckDB data warehouse for the Kumparan Data Engineer assessment.

The PostgreSQL source database is already populated with dummy `articles` data from Phase 2. Preserve the existing source schema, data, and Phase 1 environment setup.

This phase creates the warehouse structure only. Do not implement the initial data load, incremental ETL, PostgreSQL triggers, event capture, or Airflow DAGs yet.

## 1. Inspect the Existing Project

Before coding:

1. Inspect the repository structure, README, `.env.example`, `.gitignore`, dependencies, PostgreSQL schema, and Phase 2 scripts.
2. Reuse existing connection helpers, configuration patterns, naming conventions, and dependencies wherever appropriate.
3. Verify the current source schema and the actual data types of `articles`.
4. Do not overwrite existing files or introduce a second implementation of an existing feature without a clear reason.
5. If the actual source schema differs from the expected schema, document the discrepancy before making changes.

## 2. Warehouse Architecture

Use the existing local DuckDB database file as the warehouse. DuckDB is an embedded database, not a separate database server.

Create the following schemas:

- `bronze`: Raw source snapshots and captured change history.
- `silver`: The latest state of each article.
- `gold`: Analytics-ready daily metrics.
- `metadata`: Pipeline initialization status and operational metadata.

### Bronze layer

Create these tables:

**`bronze.articles_snapshot`**

Stores the initial source snapshot once the initial load is implemented in a later phase.

Requirements:
- Include all source columns: `id`, `title`, `content`, `published_at`, `author_id`, `created_at`, `updated_at`, and `deleted_at`.
- Preserve source values and timestamps without silently changing their meaning.
- Add ingestion metadata if useful, such as `ingested_at` and `snapshot_batch_id`.
- Distinguish the initial snapshot from subsequent change events.

**`bronze.article_events`**

Stores source change events captured in Phase 4.

Include fields compatible with the planned PostgreSQL event log:
- `event_id` (UUID)
- `article_id`
- `event_type` (`INSERT`, `UPDATE`, or `DELETE`)
- `payload` (JSON representation of the captured source record)
- `event_at`

Add ingestion metadata if useful. Do not implement event capture or create PostgreSQL triggers in this phase.

Bronze must eventually preserve both the initial snapshot and subsequent event history. Do not rely solely on the current Silver table to reconstruct historical data.

### Silver layer

Create **`silver.articles_current`** to represent the current state of active articles.

Requirements:
- Include the article fields from the source.
- Physically remove a row from this table when a hard-delete event is processed in a later phase.
- Do not implement SCD Type 2 or historical versions of current-state rows.
- Retain historical information in Bronze instead.
- Use sensible data types and document any warehouse-specific differences from PostgreSQL.

Do not populate Silver from PostgreSQL in this phase.

### Gold layer

Create **`gold.article_daily_metrics`** for daily activity metrics.

Use a documented date-grain definition and prepare columns for:
- `metric_date`
- `articles_created`
- `articles_published`
- `articles_deleted`
- `refreshed_at`

Define the intended semantics clearly:
- Creation date is based on `created_at`.
- Publication date is based on `published_at`.
- Deletion date is based on the captured delete event's `event_at`, not the current Silver table.
- Document how timestamps are converted into reporting dates. Use UTC consistently unless the existing assessment requirements justify another timezone.

This table will be populated by SQL transformations in a later phase. Do not implement the transformation or schedule it yet.

Because the initial snapshot cannot reveal every historical publication or deletion event, document the limits of metrics that can be reconstructed from the initial source data alone.

### Metadata layer

Create **`metadata.pipeline_state`** to persist the warehouse initialization and pipeline lifecycle state.

Include a suitable pipeline identifier, current phase/status, and timestamps for state changes.

Use documented status values such as:
- `INITIALIZING`
- `FULL_LOAD_RUNNING`
- `RECONCILING`
- `READY`
- `FAILED`

This metadata will support the initial-load and incremental-processing safeguards implemented later. Avoid adding speculative orchestration features that are not needed yet.

Also create **`metadata.processed_events`** as an idempotency checkpoint for future incremental processing. Use `event_id` as the unique key and include `processed_at`.

Document that event application, Silver mutation, and checkpoint insertion must eventually occur in the same DuckDB transaction.

## 3. Implementation

1. Create readable, maintainable DuckDB DDL SQL files in the existing project structure.
2. Add or reuse a small Python initialization entry point that opens the configured DuckDB file and executes the DDL.
3. Reuse the existing `.env` configuration and connection patterns.
4. Make initialization idempotent: running it multiple times must not duplicate objects or delete existing data.
5. Do not automatically drop schemas, tables, the DuckDB file, or PostgreSQL data.
6. Use explicit transactions where appropriate.
7. Keep business transformations in SQL files, separate from Python orchestration code.
8. Do not add pandas, an ORM, PySpark, Kafka, Debezium, cloud infrastructure, or additional dependencies unless there is a documented necessity.
9. Keep the implementation straightforward enough for the assessment author to explain line by line.

## 4. Validation

Add checks or tests to verify:

- All required DuckDB schemas and tables exist.
- The expected columns and data types exist.
- `metadata.processed_events` has an appropriate unique key on `event_id`.
- Re-running initialization succeeds without dropping existing data.
- The initialization script uses the configured DuckDB file rather than accidentally creating a database in an unexpected working directory.
- PostgreSQL source data and schema remain unchanged.

Do not insert fake warehouse rows just to make these tests pass. Do not implement the source-to-warehouse load yet.

## 5. Documentation

Update the README with:
- The Bronze, Silver, Gold, and Metadata responsibilities.
- The purpose of every table.
- The distinction between source data, initial snapshot, event history, current state, and daily aggregates.
- How to initialize the warehouse.
- How to run the validation checks.
- What is intentionally deferred to Phase 4 and Phase 5.

Explain that Bronze snapshot and event-history tables are designed to preserve history, Silver represents current active records, and Gold contains derived analytics.

## 6. Scope Restrictions

Do not implement:
- PostgreSQL triggers or the `article_event` source table.
- Initial or incremental source-to-warehouse loading.
- Event claiming, retry handling, or stale-event recovery.
- Hourly schedules or Airflow DAGs.
- Gold transformation queries.
- Cloud deployment.

These belong to subsequent phases.

## Definition of Done

- The DuckDB warehouse can be initialized locally.
- All required schemas and tables exist with documented types and purposes.
- Initialization is repeatable and does not destroy existing data.
- Validation tests pass.
- The README explains the architecture and how to run the checks.
- PostgreSQL data remains intact.
- The implementation is understandable and explainable by the assessment author.

After implementation, report the changed files, design decisions, validation commands and results, known limitations, and any deviations from this specification. Stop and wait for approval before proceeding to Phase 4.