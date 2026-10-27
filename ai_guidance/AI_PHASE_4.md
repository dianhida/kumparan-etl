# Phase 4 — Incremental ETL & Hard-Delete Synchronization

## Context

We are building a local-first Data Engineer assessment project.

Existing stack:
- PostgreSQL 16 in Docker Compose as the source database.
- DuckDB as the local analytical warehouse.
- Python with Psycopg 3 and the DuckDB Python API.
- SQL files for database objects and transformations.
- Airflow orchestration will be implemented in Phase 5.

Phases 1–3 are complete and have been reviewed. Preserve the existing project structure, configuration, naming conventions, and working functionality.

The source table is `public.articles`, with columns:
- `id`
- `title`
- `content`
- `published_at`
- `author_id`
- `created_at`
- `updated_at`
- `deleted_at`

The source uses hard deletes: a deleted article is physically removed from `public.articles`.

The DuckDB warehouse has these schemas and tables:
- `bronze.articles_snapshot`
- `bronze.article_events`
- `silver.articles_current`
- `gold.article_daily_metrics`
- `metadata.pipeline_state`
- `metadata.processed_events`

Inspect the actual repository and existing DDL before making changes. Do not assume the exact column types or metadata schema without checking the code.

## Objective

Implement PostgreSQL event capture for article INSERT, UPDATE, and DELETE operations, plus the Python functionality needed to apply captured events idempotently to DuckDB.

The solution must preserve hard-delete information even after the source article row disappears.

## Scope

### 1. PostgreSQL event capture

Create a SQL migration or initialization script that:
- Creates a PostgreSQL event table, preferably `public.article_event`.
- Captures INSERT, UPDATE, and DELETE operations on `public.articles` through a PostgreSQL trigger.
- Stores a unique event ID, article ID, event type, article payload, and event timestamp.
- Captures the NEW row for INSERT and UPDATE, and the OLD row for DELETE.
- Uses JSONB for the event payload where appropriate.
- Writes the event in the same database transaction as the article mutation.
- Does not modify the existing source table schema unless genuinely necessary.
- Can be applied repeatedly without creating duplicate triggers or failing unnecessarily.
- Does not delete existing source data or event history.

Use a database-generated UUID if supported by the existing PostgreSQL environment. If enabling an extension is necessary, explain why and verify compatibility with the Docker image.

For UPDATE, avoid generating an event when no relevant article values have changed, if this can be implemented clearly and safely.

### 2. Event metadata and error handling

Inspect the existing design before adding columns or tables.

Keep the design minimal. If event processing status is stored in PostgreSQL, use clear states such as:
- QUEUED
- PROCESSING
- SUCCEEDED
- FAILED

Add only metadata justified by the implementation, such as retry count, processing timestamp, and error details.

If error information is recorded:
- Distinguish Python processing errors from PostgreSQL errors where appropriate.
- Do not claim that a trigger can persist its own failure record when the source transaction itself rolls back.
- Avoid storing credentials or sensitive connection information in error messages.

Do not build a complicated retry framework or cleanup policy in this phase.

### 3. DuckDB event application

Implement a focused Python service or module that can:
- Read queued events from PostgreSQL.
- Parse and validate the event payload.
- Apply INSERT and UPDATE events to `silver.articles_current`.
- Remove the corresponding row from `silver.articles_current` for DELETE events.
- Preserve event history in `bronze.article_events`.
- Record successfully applied event IDs in `metadata.processed_events`.
- Use transactions so that the Bronze event record, Silver mutation, and processed-event checkpoint commit atomically within DuckDB.
- Be idempotent: reprocessing an event whose ID is already checkpointed must not apply its effects again.

Follow the actual existing DuckDB schema. Map payload fields explicitly; do not use `SELECT *` or assume PostgreSQL and DuckDB types are identical.

The implementation must not silently discard malformed events. Record actionable error information and leave the event recoverable according to the existing status design.

Do not claim end-to-end exactly-once delivery. Explain how retries and the DuckDB checkpoint prevent duplicate application under the chosen design.

### 4. Event claiming and processing

Use safe event claiming appropriate for a single local worker.

If using PostgreSQL row locks, understand that locks do not remain held after a transaction commits. Do not mark events PROCESSING and then assume the original row lock protects them throughout a separate DuckDB transaction.

Implement only the concurrency controls justified for this assessment. Avoid concurrent workers unless the design explicitly supports them.

Ensure events left in PROCESSING after a worker crash can be recovered safely, using a clear lease or stale-processing policy if needed.

### 5. Initial-load compatibility

The existing full-load workflow will be implemented or completed separately.

Document how event capture and initial snapshot loading interact:
- Event capture must be enabled before the initial snapshot begins.
- The assessment demo assumes no source mutations occur during the snapshot operation.
- After loading the snapshot, replay captured events before marking the pipeline READY.
- Do not claim that enabling a trigger before a multi-query snapshot alone guarantees a consistent snapshot under arbitrary concurrent writes.
- Do not mark the pipeline READY in this phase unless the existing workflow explicitly requires it.

Do not implement the full Airflow DAG or the complete initial snapshot workflow here.

### 6. Gold layer

Do not implement or refresh `gold.article_daily_metrics` in this phase unless existing project interfaces require a minimal compatibility change. Gold transformations belong to the transformation/orchestration phase.

## Tests

Add focused automated tests or a reproducible integration-test script covering:

1. INSERT creates an event with the expected payload.
2. UPDATE creates an event with the updated payload.
3. A no-op UPDATE does not create an unnecessary event, if supported by the chosen implementation.
4. DELETE creates an event containing the deleted article's previous values.
5. A source transaction rollback does not leave a committed article event.
6. Applying INSERT creates the active Silver row.
7. Applying UPDATE changes the active Silver row.
8. Applying DELETE removes the active Silver row.
9. Replaying the same event ID does not duplicate the event or reapply its effect.
10. A failed DuckDB transaction does not leave a processed-event checkpoint without the corresponding Bronze and Silver changes.
11. A recoverable failed event can be retried according to the implemented status policy.

Use an isolated test database or clearly separated test data. Do not delete or corrupt the existing prepared assessment dataset.

## Code quality

- Follow the existing repository structure and style.
- Prefer readable, explicit Python over unnecessary abstractions.
- Add comments explaining important transaction and retry behavior.
- Keep configuration in the existing `.env` and configuration mechanism.
- Never hardcode credentials.
- Do not introduce pandas, SQLAlchemy, PySpark, Kafka, Debezium, or cloud services.
- Do not install extra dependencies unless required and justified.
- Do not change Docker ports or unrelated infrastructure.

## Documentation and output

Update the relevant README or phase documentation with:
- Event-capture architecture.
- How to apply the SQL setup.
- How to run the event processor.
- How retries and idempotency work.
- Assumptions and limitations, especially concurrent writes and event ordering.
- Test commands and observed results.

At completion, report:
1. Files created or modified.
2. Design decisions and trade-offs.
3. Commands executed and actual test results.
4. Known limitations or failed tests.
5. Manual steps still required.

Do not begin Phase 5 or implement Airflow DAGs. Do not claim tests passed unless they were actually executed successfully.