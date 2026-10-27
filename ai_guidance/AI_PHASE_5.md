# Phase 5 — SQL Transformation & Airflow Orchestration

## Project context

We are building a local-first Data Engineer assessment project.

Existing stack:
- PostgreSQL 16 in Docker Compose as the source.
- DuckDB as the local analytical warehouse.
- Python with Psycopg 3 and the DuckDB Python API.
- SQL files for database objects and transformations.
- Airflow for orchestration.
- No cloud services or cloud warehouse.

Phases 1–4 are implemented. Phase 4 added PostgreSQL event capture, `scripts/process_events.py`, and integration tests.

Existing DuckDB schemas and tables:
- `bronze.articles_snapshot`
- `bronze.article_events`
- `silver.articles_current`
- `gold.article_daily_metrics`
- `metadata.pipeline_state`
- `metadata.processed_events`

The source table is `public.articles`. Its rows may be physically deleted, so incremental synchronization must use the captured PostgreSQL event log rather than relying only on `updated_at`.

Inspect the repository, README, Docker Compose files, SQL, Python modules, and actual schema definitions before implementing anything. Preserve existing working functionality and follow the project's conventions.

## Objective

Build two separate Airflow workflows:

1. `articles_full_load`: manually triggered initial historical load.
2. `articles_incremental_load`: scheduled hourly after the initial load has completed successfully.

Implement the SQL transformation that populates `gold.article_daily_metrics`.

The result must be restartable, idempotent where appropriate, testable, and understandable by a data engineer reviewing the code.

## 1. Initial full load

Implement a dedicated Python entry point or service for the initial load.

Requirements:
- Read all historical articles from PostgreSQL.
- Process records in bounded chunks, preferably using keyset pagination by `id`, rather than loading the entire table into memory.
- Populate `bronze.articles_snapshot` and initialize `silver.articles_current`.
- Use explicit column mappings and type conversions.
- Follow the actual schema and existing metadata design.
- Persist sufficient progress information to resume a failed load safely.
- Do not truncate or drop existing warehouse tables.
- Re-running a completed load must not duplicate records or silently overwrite unrelated warehouse data.
- Distinguish a fresh initial load from an intentional recovery or restart.

### Snapshot and event coordination

The event trigger from Phase 4 must be active before the snapshot begins.

For this assessment, document the assumption that no source mutations occur during the snapshot operation. Do not claim that enabling a trigger alone guarantees consistency under arbitrary concurrent writes.

After the snapshot is loaded:
1. Replay captured events that need to be applied after the snapshot.
2. Ensure relevant queued events have been reconciled.
3. Verify basic source and warehouse counts.
4. Mark the pipeline `READY` only after the full load and reconciliation succeed.

Do not mark the pipeline `READY` on partial failure.

Use the existing `metadata.pipeline_state` design. Avoid adding redundant metadata tables unless necessary.

## 2. Incremental workflow

Create `articles_incremental_load`, scheduled hourly.

Requirements:
- Use the existing `scripts/process_events.py` functionality instead of duplicating event-processing logic.
- Process bounded batches of queued events.
- Continue processing batches while time remains and work is available.
- Use a configurable maximum runtime of approximately 45 minutes per run.
- Leave unprocessed events queued for a later run.
- Ensure one incremental DAG run cannot overlap another.
- Check pipeline readiness before processing.
- Do not process incrementals while the initial load or reconciliation is incomplete.
- Recover stale events using the existing recovery policy.
- Preserve the existing DuckDB checkpoint and PostgreSQL event-status behavior.

If the existing processor cannot safely support the required runtime or batch controls, make the smallest necessary change and add tests.

Do not create concurrent workers against the same DuckDB file.

Do not claim exactly-once delivery across PostgreSQL and DuckDB. Document the idempotent retry behavior.

## 3. Gold transformation

Implement a readable SQL transformation for `gold.article_daily_metrics`.

The target columns are:
- `metric_date`
- `articles_created`
- `articles_published`
- `articles_deleted`
- `refreshed_at`

Metric definitions:
- `articles_created`: number of article creation events by `created_at` date.
- `articles_published`: number of articles with a non-null `published_at`, grouped by publication date.
- `articles_deleted`: number of captured DELETE events grouped by event date.
- Use a consistent UTC reporting date convention.
- Do not count unpublished articles as published.
- Avoid multiplying counts when joining the different activity sources.
- Include dates that have activity in any metric, even if another metric is zero.
- Refresh the affected date range safely and make repeated transformations idempotent.

Use `bronze.articles_snapshot` for historical creation and publication activity available in the initial snapshot, and use `bronze.article_events` for subsequent changes.

Be explicit about limitations: a snapshot cannot reconstruct articles deleted before the snapshot or historical publication states that were never captured. Do not invent missing history.

If UPDATE events can change `created_at` or `published_at`, handle the old and new affected dates correctly where the available event payloads permit it. Avoid counting the same creation event twice when snapshot and event history overlap.

Use a SQL file for the transformation. Keep orchestration code separate from analytical SQL.

## 4. Airflow setup and DAG design

Inspect the current environment before deciding how to add Airflow. Reuse existing configuration if it is already installed.

If Airflow is not configured, implement the smallest local Docker Compose setup that is compatible with the project's existing stack. Avoid unnecessary services.

Requirements:
- Store DAG files in the appropriate Airflow DAG directory.
- Keep credentials in environment configuration or Airflow connections, never hardcoded in DAG code.
- Use clear task dependencies and retries.
- Set timeouts and prevent overlapping runs.
- Ensure task failures are visible in Airflow.
- Do not put large ETL implementations directly inside DAG files; DAGs should orchestrate reusable Python functions or scripts.
- Ensure Airflow containers can reach PostgreSQL and access the same DuckDB file through a consistent mounted path.
- Ensure the PostgreSQL hostname used by Airflow is appropriate for Docker networking; do not assume `localhost` refers to the PostgreSQL container.
- Account for DuckDB file locking and avoid concurrent writers.

The full-load DAG should be manually triggered. The incremental DAG should run hourly but safely skip processing until the full load and reconciliation are complete.

Do not use an hourly schedule for the full load.

## 5. Data quality checks

Add checks after full load and Gold refresh:
- Source article count versus snapshot count, accounting for the documented consistency assumption.
- No duplicate article IDs in the snapshot or Silver.
- Silver contains only active articles.
- Gold metrics are nonnegative.
- `metric_date` is unique in Gold.
- Re-running Gold transformation produces the same metric results.
- A failed load does not incorrectly mark the pipeline `READY`.

Use the existing testing conventions and isolated test data where appropriate. Do not damage the prepared assessment dataset.

## 6. Operational safety

- Keep all existing Phase 1–4 tests passing.
- Do not drop or truncate existing warehouse tables as part of normal execution.
- Do not silently reset event statuses or processed-event checkpoints.
- Do not automatically rerun the full load on every hourly execution.
- Make the full-load recovery behavior explicit.
- Keep the implementation simple enough to explain during a technical interview.

## 7. Documentation and deliverables

Update the README with:
- Architecture and DAG responsibilities.
- How to start Airflow locally.
- How to trigger the initial full load.
- How to verify that the pipeline is `READY`.
- How hourly incremental processing works.
- How to inspect Bronze, Silver, and Gold in DuckDB.
- How to run tests.
- Known assumptions and limitations.
- A clear explanation of AI-assisted development, design decisions, and trade-offs.

At completion, report:
1. All created or modified files.
2. The full-load and incremental workflow design.
3. Commands actually executed and their results.
4. Tests passed and tests not run.
5. Any unresolved bugs or limitations.

Do not begin Phase 6 as a separate implementation phase. Do not claim successful execution unless the workflow was actually run and verified.