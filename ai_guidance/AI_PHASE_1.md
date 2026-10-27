# Phase 1 — Environment Setup
## Kumparan Data Engineer Assessment

### 1. Role and objective

You are an AI coding agent assisting with a Data Engineer technical assessment for Kumparan.

Your task in Phase 1 is to inspect the existing development environment and prepare the local project environment for subsequent ETL development.

Prioritize:
- A working, reproducible local environment.
- Simple, readable code and configuration.
- Minimal dependencies.
- Clear setup and verification instructions.
- No unnecessary cloud services or infrastructure.

Do not implement the actual ETL pipeline in this phase.

### 2. Project context

The assessment requires a data pipeline that migrates data from a source database into a data warehouse, supports incremental updates and hard deletes, and runs hourly through Airflow.

The source table is `articles`, with these fields:

- `id`
- `title`
- `content`
- `published_at`
- `author_id`
- `created_at`
- `updated_at`
- `deleted_at`

The implementation will use PostgreSQL as the source and DuckDB as the local analytical warehouse.

### 3. Technology stack

Use the following stack:

| Component | Technology |
|---|---|
| Development environment | WSL2 with Ubuntu |
| Container runtime | Docker Desktop with WSL2 integration |
| Source database | PostgreSQL in Docker |
| Warehouse | DuckDB local database file |
| Programming language | Python |
| PostgreSQL driver | Psycopg 3 |
| Warehouse access | DuckDB Python API |
| Configuration | Environment variables and `.env` |
| Version control | Git |
| Orchestration | Apache Airflow, implemented in a later phase |
| Transformations | SQL files, implemented in later phases |
| Testing | pytest, introduced as needed |

Do not introduce pandas, SQLAlchemy, PySpark, Kafka, Debezium, or cloud infrastructure unless explicitly requested.

DuckDB is an embedded analytical database. It does not need a separate database server container or network port.

### 4. Environment assumptions

Before changing anything, inspect the available environment.

Check:
- Whether WSL2 and Ubuntu are available.
- Whether Docker Desktop is installed and running.
- Whether WSL integration is enabled.
- Whether Docker Compose is available.
- Whether Python 3 and Git are installed.
- Whether an existing project directory or files need to be preserved.

Do not assume these components are installed simply because they are expected.

If a prerequisite is missing, explain the required action and provide the appropriate commands. Do not claim that a component was installed or verified unless it was actually checked.

### 5. Repository location and structure

Keep the project inside the Linux filesystem, for example:

`~/workspace/kumparan-assessment`

Avoid developing inside `/mnt/c/...` unless there is a specific reason.

Use a simple initial structure:

```text
kumparan-assessment/
├── .env.example
├── .gitignore
├── docker-compose.yml
├── requirements.txt
├── README.md
├── scripts/
├── src/
├── sql/
├── dags/
├── tests/
└── data/
```

Create only the directories and files needed for Phase 1. Empty directories do not need placeholder files unless required by the chosen implementation.

Do not overwrite existing user files without inspecting them first.

### 6. Phase 1 implementation requirements

#### A. Docker and PostgreSQL

Prepare a Docker Compose configuration for a local PostgreSQL instance.

Requirements:
- Use a pinned, supported PostgreSQL image version.
- Read database credentials and configuration from environment variables.
- Provide a `.env.example` containing placeholder values, never real credentials.
- Publish PostgreSQL on a configurable host port, defaulting to `5432`.
- Use a persistent Docker volume so database data survives container recreation.
- Include a health check.
- Provide clear startup, status-check, shutdown, and connection-verification commands.

Do not configure production credentials or expose the database publicly.

Do not delete database volumes or reset existing data as part of routine setup.

#### B. Python environment

Create a Python virtual environment inside the project.

Install only the dependencies needed for the initial environment:
- `psycopg[binary]`
- `duckdb`
- `python-dotenv`
- `pytest`

Pin dependency versions appropriately or use a documented, reproducible dependency strategy.

Use the virtual environment for project commands. Do not install project dependencies globally.

#### C. DuckDB verification

Verify that Python can:
1. Import DuckDB.
2. Create a local DuckDB database file.
3. Create and query a temporary test table.
4. Close the connection cleanly.

Keep the database file in the project’s designated local data directory, which should be excluded from Git if it contains generated data.

Do not create the final Bronze, Silver, or Gold schemas in Phase 1.

#### D. PostgreSQL connectivity verification

Verify that Python can connect to the Dockerized PostgreSQL instance using environment-based configuration.

The verification should:
1. Establish a connection.
2. Execute a simple query such as `SELECT version()`.
3. Print a successful connection result.
4. Close the connection reliably.

Do not create the `articles` table, event log, or trigger in this phase.

#### E. Git and secrets

Prepare `.gitignore` to exclude at least:
- `.env`
- Python virtual environments
- `__pycache__/`
- `.pytest_cache/`
- Generated DuckDB files
- Generated local data
- Logs and temporary files

Keep `.env.example` tracked and free of real secrets.

### 7. Airflow scope

Do not install or configure the complete Airflow stack in Phase 1.

Airflow DAGs, scheduler, webserver, orchestration dependencies, retries, concurrency controls, and hourly scheduling will be implemented in a later phase.

The environment setup should not become unnecessarily complex just to demonstrate Airflow installation.

### 8. Architecture decisions already established

These decisions are fixed for subsequent phases unless explicitly changed by the user.

#### Source and event capture
- PostgreSQL is the source of truth.
- A PostgreSQL trigger will capture `INSERT`, `UPDATE`, and `DELETE` operations.
- Events will be stored in `article_event`.
- Event identifiers will use UUIDs.
- Event payloads will use JSONB.
- DELETE events must preserve the deleted row’s previous values.

#### Error handling
- `exception_msg` records PostgreSQL errors when they can be captured and persisted.
- `python_msg` records Python processing errors.
- Errors are populated only when a relevant failure occurs.
- Events are retained; there is no automatic event-log cleanup in the assessment.

#### Warehouse
- DuckDB is the destination.
- Bronze retains the initial snapshot and event history.
- Silver contains the current state of active articles.
- Gold contains daily article metrics.
- A DuckDB checkpoint records processed event UUIDs.

#### Incremental processing
- Event capture uses triggers; no CDC/WAL reader will be implemented initially.
- Incremental processing polls PostgreSQL events through Python.
- Each batch processes at most 100 events.
- The intended delay between batches is 10 seconds after a batch completes.
- Each incremental execution has a maximum duration of 45 minutes.
- The hourly Airflow schedule will be implemented later.
- Event application and checkpoint insertion must share one DuckDB transaction.
- PostgreSQL event status is updated to `SUCCEEDED` after destination success is confirmed.

#### Full Load and Incremental Load
These are separate Airflow workflows:
- `articles_full_load`: manually initiated initial snapshot, processed in chunks and resumable.
- `articles_incremental_load`: scheduled hourly after initialization is complete.

The incremental workflow must not process events while Full Load or reconciliation is in progress. A persistent pipeline phase and appropriate concurrency controls will be implemented later.

For the assessment, Full Load assumes source data remains unchanged during the snapshot. This limitation must be documented; the trigger-based approach does not guarantee correct commit ordering for concurrent source transactions.

### 9. Explicit exclusions for Phase 1

Do not implement:
- The `articles` source table.
- Dummy article data.
- `article_event` or PostgreSQL triggers.
- Bronze, Silver, or Gold schemas.
- Full Load logic.
- Incremental ETL logic.
- Event claiming, retries, or stale-event recovery.
- Airflow DAGs.
- SQL transformations.
- Daily metrics.
- CDC/WAL replication.
- Cloud deployment.
- Production monitoring infrastructure.

These belong to subsequent phases.

### 10. Definition of done

Phase 1 is complete only when:

1. The project directory exists in the intended Linux filesystem.
2. Docker and Docker Compose are verified.
3. PostgreSQL starts successfully and passes its health check.
4. PostgreSQL data persists across a normal container restart.
5. The Python virtual environment is created and usable.
6. The required dependencies can be imported.
7. Python can connect to PostgreSQL and execute a query.
8. Python can create and query a local DuckDB database.
9. `.env.example` and `.gitignore` are present and correct.
10. The README documents prerequisites, setup, verification, common errors, and cleanup commands.
11. No actual ETL pipeline has been implemented prematurely.
12. No existing user data or files have been deleted.

### 11. Agent working instructions

Work incrementally.

1. Inspect the environment and existing files.
2. Report missing prerequisites and any potential conflicts.
3. Implement the minimum necessary Phase 1 changes.
4. Run available verification commands.
5. Fix errors caused by the implementation.
6. Report which checks passed, failed, or could not be run.
7. Summarize changed files and provide exact commands for the user to continue.

Explain important technical decisions in concise, understandable language. Prefer straightforward Python and SQL over abstractions that are unnecessary for this assessment.

Do not claim successful execution based on inspection alone.

Do not proceed to Phase 2 without explicit user approval.

**Primary objective:** deliver a reproducible local development environment that is ready for Phase 2 data preparation, without expanding the scope beyond environment setup.
