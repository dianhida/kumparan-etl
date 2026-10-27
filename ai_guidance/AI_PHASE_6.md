# Phase 6 — Airflow Reliability and Final Validation

Continue the existing Kumparan Data Engineer assessment project. Do not introduce new infrastructure or technologies.

## Objectives

1. Review and validate `articles_full_load` and `articles_incremental_load`.
2. Fix confirmed reliability issues without unnecessary refactoring.
3. Prevent full-load and incremental DAGs from writing to the DuckDB warehouse concurrently.
4. Verify retry behavior for partially completed full loads. Make the load safely resumable or implement an explicit, safe recovery procedure.
5. Verify failed events cannot cause an infinite processing loop. Failed events must remain observable and must not be silently marked successful.
6. Ensure incremental processing stops within its configured runtime limit and leaves unprocessed events available for subsequent scheduled runs.
7. Ensure Gold transformation only runs after its upstream processing task succeeds.
8. Add automated tests for DAG importability, task dependencies, concurrency protection, failure handling, and idempotent recovery where practical.
9. Run existing QC tests and all relevant regression tests.
10. Update README with the actual test commands, results, known limitations, and recovery procedures.

## Constraints

- Preserve the existing Python, PostgreSQL, DuckDB, and Airflow stack.
- Do not add cloud services, Kafka, Spark, or unnecessary dependencies.
- Do not delete warehouse data or WAL files as a recovery shortcut.
- Do not claim a test passed unless it was actually executed.
- Keep changes focused and explain the reason for every significant modification.

## Deliverables

- Modified code and SQL only where needed.
- Test results with pass/fail counts.
- A concise list of confirmed bugs, fixes, and remaining limitations.
- Instructions for safely running the initial full load and hourly incremental pipeline.

First inspect the existing implementation, particularly `scripts/full_load.py`, `scripts/process_events.py`, and `scripts/run_gold.py`. Then implement and test the smallest reliable solution.