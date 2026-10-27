# Phase 7 — Airflow Deployment & Final Acceptance Test

Continue the existing Kumparan Data Engineer assessment project. Phase 7 is the final implementation and validation phase.

## Objectives

1. Inspect the current repository, README, Docker Compose configuration, Airflow DAGs, and existing tests before making changes.
2. Make Airflow runnable locally through Docker Compose, using a pinned, compatible Airflow image and the smallest practical configuration.
3. Preserve the existing PostgreSQL source database, its data, and the DuckDB warehouse. Do not reset, truncate, or recreate existing data as a shortcut.
4. Ensure Airflow can import both DAGs and their Python dependencies.
5. Ensure the Airflow containers and locally executed scripts access the same intended DuckDB warehouse file. Configure shared paths explicitly.
6. Ensure the full-load and hourly incremental DAGs share effective DuckDB concurrency protection. Verify that the file lock works across the actual processes and containers that use the warehouse.
7. Configure Airflow metadata storage without accidentally using or modifying the Kumparan source tables. Prefer isolated metadata storage; do not expose unnecessary database ports.
8. Provide a safe, repeatable startup and initialization procedure. Do not initialize Airflow metadata or create databases on every container restart.
9. Verify the full-load DAG's state checks, retry/resume behavior, reconciliation, Gold transformation, and final READY state.
10. Verify the hourly incremental DAG, including INSERT, UPDATE, and hard-DELETE propagation, bounded runtime, failed-event handling, and Gold transformation dependencies.
11. Verify that Gold runs only after successful upstream processing and does not run after an upstream failure.
12. Test cross-DAG locking and recovery from a failed or interrupted run wherever practical.
13. Run the existing Phase 4 integration tests, Phase 5 regression tests, quality checks, and Phase 6 DAG tests in the actual supported environment.
14. Reconcile README claims with observed results. Correct contradictions, document actual commands and pass/fail counts, and distinguish executed tests from untested behavior.
15. Document how to start and stop Airflow, access the UI, trigger the initial full load, monitor hourly incremental runs, inspect failures, and recover safely.

## Constraints

- Preserve Python, PostgreSQL, DuckDB, and Airflow as the core stack.
- Do not add Kafka, Spark, cloud services, or unnecessary dependencies.
- Pin compatible dependency and container versions; avoid unbounded versions such as `latest`.
- Keep the current source PostgreSQL data and existing DuckDB warehouse intact.
- Do not delete DuckDB WAL files or use destructive recovery shortcuts.
- Do not hardcode secrets in source code or commit `.env`.
- Avoid unnecessary refactoring. Explain each significant modification.
- Do not claim tests passed unless they were actually executed.
- Do not require Windows PostgreSQL to be uninstalled or change unrelated services.
- Ensure containers have appropriate health checks, startup dependencies, and persistent storage where needed.
- Do not mark a pipeline READY unless the required processing and reconciliation have actually succeeded.

## Final acceptance criteria

- Airflow web UI and scheduler start successfully.
- Both DAGs appear in the UI and can be imported without errors.
- The full-load DAG can run safely against the existing dataset or verify an already-completed load without duplicating data.
- The incremental DAG processes new events idempotently and respects its runtime and retry limits.
- Full load and incremental processing cannot write to the DuckDB warehouse concurrently.
- Gold transformation runs only after successful upstream tasks.
- Existing warehouse data remains consistent, and all applicable regression and quality tests have recorded results.
- README accurately describes the final architecture, setup, test results, known limitations, and recovery procedures.

## Deliverables

1. Minimal code and Docker Compose changes required to run Airflow.
2. A clear startup and initialization guide.
3. Actual test results, including failures and skipped tests.
4. A concise final assessment of confirmed bugs, fixes, remaining limitations, and whether the project meets the acceptance criteria.

Start by inspecting the repository and current runtime. Implement the smallest safe solution, then validate it incrementally. Do not assume the README is proof that every feature or test currently works.