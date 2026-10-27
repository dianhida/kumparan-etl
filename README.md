# Kumparan Data Engineer Assessment

Environment setup and data preparation for the Kumparan Data Engineer technical assessment.

## Scenario-based

This implementation addresses the assessment scenario with a **single source table: `articles`** (as specified in the assessment). 

### Scenario Assumptions

| Aspect | Assumption | Rationale |
|--------|------------|-----------|
| **Source** | Single table `public.articles` | Assessment specifies only the articles table |
| **Gold Layer** | Built from available data only | No external dimensions/facts - metrics derived purely from `articles` + captured events |
| **Trigger/Event Capture** | PostgreSQL AFTER trigger on `articles` | Captures INSERT/UPDATE/DELETE with full row payload |
| **Bronze Layer** | Snapshot + Event log | Preserves both initial state and full change history |
| **Silver Layer** | Current active articles only | Hard deletes physically remove rows; history in Bronze |
| **Gold Layer** | Daily metrics (created/published/deleted) | Derived from Bronze snapshot + event log |

### Architecture Flow

```
PostgreSQL (articles) 
    │
    ├─► AFTER Trigger → public.article_event (UUID, JSONB payload)
    │
    ▼
Full Load (keyset pagination) ──► Bronze: articles_snapshot + article_events
    │
    ▼
Silver: articles_current (active only)
    │
    ▼
Gold: article_daily_metrics (daily aggregates)
```

### Hard Delete Handling
- PostgreSQL uses **hard delete** (row physically removed)
- Trigger captures **OLD row** before deletion → stored in `article_event` with `event_type='DELETE'`
- Incremental processor **physically removes** row from Silver
- Bronze preserves full history (snapshot + DELETE event)
- Gold tracks `articles_deleted` by event date

---

## Prerequisites

- WSL2 with Ubuntu
- Docker Desktop with WSL2 integration
- Docker Compose
- Python 3.10+
- Git

## Quick Start

### 1. Clone and navigate to project

```bash
cd ~/workspace/kumparan_etl
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env if you need custom values (defaults work for local development)
```

### 3. Start PostgreSQL and pgAdmin

```bash
docker-compose up -d
```

### 4. Verify services are healthy

```bash
docker-compose ps
docker-compose logs postgres
docker-compose logs pgadmin
```

Access pgAdmin at: http://localhost:5050
- Email: admin@kumparan.com
- Password: admin

### DuckDB UI (Optional)

The DuckDB Python package includes a local web UI extension (experimental):

```bash
# Install and check UI status
.venv/bin/python -c "import duckdb; conn = duckdb.connect('data/warehouse.duckdb'); conn.execute('INSTALL ui; LOAD ui;'); print(conn.execute('SELECT * FROM ui_is_started()').fetchall())"
```

**Start the UI server** (runs on http://localhost:4213):
```bash
.venv/bin/python -c "
import duckdb
conn = duckdb.connect('data/warehouse.duckdb')
conn.execute('LOAD ui')
conn.execute('CALL start_ui_server()')
print('UI server started at http://localhost:4213')
input('Press Enter to stop...')
"
```

**Note**: The `INSTALL ui` step only needs to run once. After that, `LOAD ui` and `CALL start_ui_server()` will start the web UI. Press Enter to stop the server.

### 5. Create and activate Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 6. Run Phase 1 verifications

```bash
# Verify DuckDB
python scripts/verify_duckdb.py

# Verify PostgreSQL connectivity
python scripts/verify_postgres.py
```

### 7. Run Phase 2: Data Preparation

```bash
# Create articles table schema
docker-compose exec -T postgres psql -U kumparan -d kumparan < sql/01_create_articles_table.sql

# Generate dummy data (2016-2026, ~11,000 articles)
python scripts/generate_dummy_data.py

# Run validation queries
docker-compose exec -T postgres psql -U kumparan -d kumparan < sql/02_validate_data.sql
```

### 8. Run Phase 3: Warehouse Initialization

```bash
# Initialize DuckDB warehouse (Bronze, Silver, Gold, Metadata schemas)
python scripts/init_warehouse.py

# Validate warehouse
python scripts/validate_warehouse.py
```

### 9. Run Phase 4: Event Capture & Processing

```bash
# 1. Apply PostgreSQL event capture (idempotent)
docker-compose exec -T postgres psql -U kumparan -d kumparan < sql/09_article_event_capture.sql

# 2. Run event processor (processes all QUEUED events)
python scripts/process_events.py

# 3. Run integration tests
python scripts/test_phase4.py
```

### 10. Run Phase 5: Full Load, Incremental & Gold

```bash
# 1. Run initial full load (PostgreSQL → Bronze snapshot + Silver)
python scripts/full_load.py

# 2. Run gold transformation
python scripts/run_gold.py

# 3. Run incremental load (processes new events)
python scripts/process_events.py

# 4. Run quality checks
python scripts/quality_checks.py
```


### 11. Run Phase 5 Tests

```bash
# Run Phase 5 integration tests (includes trigger scenarios)
docker exec -u 1000 kumparan-airflow-webserver python3 /opt/airflow/scripts/test_phase5.py

# Or locally (requires permission fix - see below)
.venv/bin/python scripts/test_phase5.py
```

### Phase 5 Test Coverage

The `scripts/test_phase5.py` covers 6 test scenarios:

| Test | Description |
|------|-------------|
| Test 1 | Bronze snapshot → Silver only active articles |
| Test 2 | Gold metrics consistency with source (PG active, published, deleted) |
| Test 3 | Hard delete scenario - DELETE removes from Silver, preserves in Bronze, updates Gold |
| Test 4 | UPDATE scenario - Silver updated, event captured in Bronze |
| Test 5 | No-op UPDATE creates no event |
| Test 6 | Trigger scenarios (INSERT, UPDATE, DELETE, rollback) |

**⚠️ Permission Issue (Important):**  
The DuckDB file at `data/warehouse.duckdb` is owned by the Airflow container user (UID 1000). Running tests locally with your user will fail with `Permission denied` on the DuckDB file.

**Solution:** Run tests inside the Airflow container as user 1000:
```bash
docker exec -u 1000 kumparan-airflow-webserver python3 /opt/airflow/scripts/test_phase5.py
```

Or fix permissions locally (requires sudo):
```bash
sudo chown -R $USER:$USER data/
```

Run tests inside the container for consistent results.

### 12. Run Phase 6 Tests

```bash
docker exec -u 1000 kumparan-airflow-webserver python3 /opt/airflow/scripts/test_dags.py
```
## Project Structure

```
kumparan_etl/
├── .env.example          # Environment template (tracked)
├── .env                  # Local environment (NOT tracked)
├── .gitignore            # Git ignore rules
├── docker-compose.yml    # PostgreSQL + pgAdmin services
├── requirements.txt      # Python dependencies
├── README.md             # This file
├── scripts/              # Verification and utility scripts
│   ├── verify_duckdb.py         # Phase 1: DuckDB verification
│   ├── verify_postgres.py       # Phase 1: PostgreSQL verification
│   ├── generate_dummy_data.py   # Phase 2: Deterministic data generator
│   ├── init_warehouse.py        # Phase 3: Warehouse initialization
│   ├── validate_warehouse.py    # Phase 3: Warehouse validation
│   ├── process_events.py        # Phase 4: Event processor
│   ├── test_phase4.py           # Phase 4: Integration tests
│   ├── full_load.py             # Phase 5: Initial full load
│   ├── run_gold.py              # Phase 5: Gold transformation
│   ├── quality_checks.py        # Phase 5: Data quality checks
│   ├── duckdb_lock.py           # Phase 6: DuckDB file locking
│   └── test_dags.py             # Phase 6: DAG importability tests
├── src/                  # Source code (Phase 7+)
├── sql/                  # SQL scripts
│   ├── 01_create_articles_table.sql   # Phase 2: Articles schema
│   ├── 02_validate_data.sql         # Phase 2: Validation queries
│   ├── 03_test_scenarios.sql        # Phase 2: Controlled test scenarios
│   ├── 04_bronze_schema.sql         # Phase 3: Bronze layer
│   ├── 05_silver_schema.sql         # Phase 3: Silver layer
│   ├── 06_gold_schema.sql           # Phase 3: Gold layer
│   ├── 07_metadata_schema.sql       # Phase 3: Metadata layer
│   ├── 08_init_warehouse.sql        # Phase 3: Master init (legacy)
│   ├── 09_article_event_capture.sql # Phase 4: PostgreSQL event capture
│   └── 10_gold_transformation.sql   # Phase 5: Gold transformation
├── dags/                 # Airflow DAGs (Phase 6)
│   ├── articles_full_load.py
│   └── articles_incremental_load.py
├── tests/                # Test files
└── data/                 # Local data files (DuckDB, etc.)
```

## Verification Commands

| Check | Command |
|-------|---------|
| Docker Compose status | `docker-compose ps` |
| PostgreSQL logs | `docker-compose logs postgres` |
| pgAdmin logs | `docker-compose logs pgadmin` |
| PostgreSQL health | `docker-compose exec postgres pg_isready -U kumparan -d kumparan` |
| pgAdmin UI | Open http://localhost:5050 (admin@kumparan.com / admin) |
| Python imports | `.venv/bin/python -c "import psycopg, duckdb, dotenv; print('OK')"` |
| DuckDB verification | `python scripts/verify_duckdb.py` |
| PostgreSQL verification | `python scripts/verify_postgres.py` |
| Warehouse initialization | `python scripts/init_warehouse.py` |
| Warehouse validation | `python scripts/validate_warehouse.py` |
| Event capture setup | `docker-compose exec -T postgres psql -U kumparan -d kumparan < sql/09_article_event_capture.sql` |
| Event processor | `python scripts/process_events.py` |
| Phase 4 tests | `python scripts/test_phase4.py` |
| Full load | `python scripts/full_load.py` |
| Gold transformation | `python scripts/run_gold.py` |
| Incremental load | `python scripts/process_events.py` |
| Quality checks | `python scripts/quality_checks.py` |
| DAG tests | `python scripts/test_dags.py` |
| Run tests | `python -m pytest tests/ -v` |

## Phase 2: Data Preparation

### Schema

The `articles` table is created with:
- `id` - BIGSERIAL primary key
- `title` - TEXT NOT NULL
- `content` - TEXT NOT NULL
- `published_at` - TIMESTAMPTZ (nullable for unpublished)
- `author_id` - INTEGER > 0
- `created_at` - TIMESTAMPTZ NOT NULL DEFAULT NOW()
- `updated_at` - TIMESTAMPTZ NOT NULL DEFAULT NOW() (constrained: `updated_at >= created_at`)
- `deleted_at` - TIMESTAMPTZ (NULL for initial dataset)

Indexes on `created_at`, `published_at`, `author_id`, `deleted_at`.

### Dummy Data Generation

Deterministic generator (`scripts/generate_dummy_data.py`):
- Fixed seed (42) for reproducibility
- ~10,000 articles from 2016-2025
- ~1,000 articles from 2026
- Varied titles, content, author_ids (1-50)
- 80% published, 20% unpublished
- Realistic timestamp relationships (`updated_at >= created_at`, `published_at` between them)
- Batch inserts (500 rows/batch) with parameterized queries
- Idempotent: refuses to run if table already has data

### Validation Results (Expected)

| Check | Expected |
|-------|----------|
| Total articles | 11,000 |
| Date range | 2016-01-01 to 2026-12-31 |
| Published | ~8,900 (80%) |
| Unpublished | ~2,100 (20%) |
| Unique authors | 50 |
| Invalid timestamps | 0 |
| Duplicate IDs | 0 |
| Non-NULL deleted_at | 0 |

### Re-generating Data

To regenerate, explicitly truncate first:
```bash
docker-compose exec -T postgres psql -U kumparan -d kumparan -c "TRUNCATE TABLE articles RESTART IDENTITY;"
python scripts/generate_dummy_data.py
```

### Controlled Test Scenarios

Documented in `sql/03_test_scenarios.sql` (not auto-executed):
1. Insert new article
2. Update existing article
3. Hard-delete article
4. Insert unpublished article
5. Update publication timestamp

All test records use `TEST_` prefix for easy identification and cleanup.

## Phase 3: Warehouse Design & Initialization

### Architecture Overview

The DuckDB warehouse implements a **Medallion Architecture** with four layers:

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Bronze** | `bronze` | Raw source snapshots and captured change history. Preserves history. |
| **Silver** | `silver` | Current state of active articles. Physical deletes for hard-deletes. |
| **Gold** | `gold` | Analytics-ready daily metrics. Derived from Bronze/Silver. |
| **Metadata** | `metadata` | Pipeline state and idempotency checkpoints. |

### Bronze Layer

**`bronze.articles_snapshot`**
- Stores initial source snapshot (all source columns: `id`, `title`, `content`, `published_at`, `author_id`, `created_at`, `updated_at`, `deleted_at`)
- Ingestion metadata: `ingested_at`, `snapshot_batch_id`, `is_initial_snapshot`
- Primary key: `(id, snapshot_batch_id)` to support multiple snapshots

**`bronze.article_events`**
- Stores change events from PostgreSQL triggers (implemented in Phase 4)
- Fields: `event_id` (UUID), `article_id`, `event_type` (INSERT/UPDATE/DELETE), `payload` (JSON), `event_at`
- Ingestion metadata: `ingested_at`, `processed_at`
- Primary key: `event_id`

> Bronze preserves both initial snapshot AND event history. Silver alone cannot reconstruct historical data.

### Silver Layer

**`silver.articles_current`**
- Current state of active (non-deleted) articles
- Same article fields as source, plus `last_synced_at`
- Rows physically deleted when hard-delete events processed
- No SCD Type 2 - historical versions retained in Bronze
- Primary key: `id`

### Gold Layer

**`gold.article_daily_metrics`**
- Daily activity metrics (UTC date grain)
- Columns: `metric_date`, `articles_created`, `articles_published`, `articles_deleted`, `refreshed_at`
- Creation date from `created_at`, publication from `published_at`, deletion from event `event_at`
- Initial snapshot limitations: pre-snapshot deletes unknown; historical publications only from `published_at`

### Metadata Layer

**`metadata.pipeline_state`**
- Pipeline lifecycle state: `INITIALIZING` → `FULL_LOAD_RUNNING` → `RECONCILING` → `READY` / `FAILED`
- Supports initial-load and incremental-processing safeguards
- Progress tracking: `snapshot_batch_id`, `loaded_count`, `last_article_id`

**`metadata.processed_events`**
- Idempotency checkpoint for incremental processing
- Primary key: `event_id` (matches `bronze.article_events.event_id`)
- **Critical**: Event application, Silver mutation, and checkpoint insertion must share same DuckDB transaction

### Initialization

```bash
# Initialize warehouse (idempotent)
python scripts/init_warehouse.py

# Validate warehouse
python scripts/validate_warehouse.py
```

### Validation Results (Expected)

| Check | Expected |
|-------|----------|
| Schemas | bronze, silver, gold, metadata |
| Tables | 6 tables (2 per layer except metadata: 2) |
| bronze.articles_snapshot | 11 columns, PK on (id, snapshot_batch_id) |
| bronze.article_events | 7 columns, PK on event_id |
| silver.articles_current | 9 columns, PK on id |
| gold.article_daily_metrics | 5 columns, PK on metric_date |
| metadata.processed_events | PK on event_id |
| metadata.pipeline_state | 1 row, phase=INITIALIZING |
| Idempotency | Re-run succeeds, no data loss |
| DuckDB location | `data/warehouse.duckdb` |
| PostgreSQL source | Unchanged (11,000 articles) |

## Phase 4: Incremental ETL & Hard-Delete Synchronization

### Event Capture Architecture

**PostgreSQL Side:**
- `public.article_event` table stores captured events with UUID, article_id, event_type, JSONB payload, event_at
- Trigger `trg_articles_capture_event` fires AFTER INSERT/UPDATE/DELETE on `public.articles`
- Captures NEW row for INSERT/UPDATE, OLD row for DELETE (preserves deleted data)
- No-op UPDATEs (no relevant column changes) don't generate events
- Transactional: event written in same transaction as article mutation
- Processing status: `QUEUED` → `PROCESSING` → `SUCCEEDED` / `FAILED`
- Retry count, error messages (`python_msg`, `exception_msg`), timestamps tracked

**DuckDB Side:**
- `bronze.article_events`: Raw event history (event_id, article_id, event_type, payload, event_at, ingested_at)
- `silver.articles_current`: Current active articles (upsert on INSERT/UPDATE, hard delete on DELETE)
- `metadata.processed_events`: Idempotency checkpoint (PK on event_id)
- Single DuckDB transaction for Bronze insert + Silver mutation + checkpoint

### Setup

```bash
# 1. Apply PostgreSQL event capture (idempotent)
docker-compose exec -T postgres psql -U kumparan -d kumparan < sql/09_article_event_capture.sql

# 2. Run event processor (processes all QUEUED events)
python scripts/process_events.py
```

### Event Processor (`scripts/process_events.py`)

- Claims QUEUED events using `FOR UPDATE SKIP LOCKED` (safe for single worker)
- Releases stale PROCESSING events (>5 min) back to QUEUED for retry
- Idempotent: skips events already in `metadata.processed_events`
- Applies events in DuckDB transaction (Bronze + Silver + checkpoint atomic)
- Marks events SUCCEEDED/FAILED in PostgreSQL
- Batch size: 100 (configurable via `EVENT_BATCH_SIZE`)

### Retry & Idempotency

| Mechanism | Purpose |
|-----------|---------|
| `metadata.processed_events` PK on `event_id` | Prevents duplicate application |
| `FOR UPDATE SKIP LOCKED` | Safe event claiming without deadlocks |
| Stale event release (5 min) | Recovers from worker crashes |
| Status: FAILED → QUEUED | Manual or automated retry |
| Transaction atomicity | Bronze + Silver + checkpoint commit together |

**Exactly-once delivery**: Not guaranteed end-to-end, but at-least-once with idempotent application ensures correct state. Duplicate events are detected by checkpoint and skipped.

### Initial-Load Compatibility

1. Enable trigger BEFORE initial snapshot begins
2. Run full load (PostgreSQL → Bronze snapshot) - assumes no source mutations during snapshot
4. Replay captured events to Silver
5. Mark pipeline READY

**Limitation**: Trigger before snapshot doesn't guarantee consistent snapshot under concurrent writes. Assessment assumes no mutations during snapshot.

### Test Results (All Passed)

| Test | Description |
|------|-------------|
| 1 | INSERT creates event with expected payload |
| 2 | UPDATE creates event with updated payload |
| 3 | No-op UPDATE creates no event |
| 4 | DELETE creates event with old values |
| 5 | Rollback leaves no committed event |
| 6 | INSERT applied to Silver |
| 7 | UPDATE changes Silver row |
| 8 | DELETE removes Silver row |
| 9 | Idempotent replay (no duplicates) |
| 10 | Failed transaction atomicity |
| 11 | Failed event retry |

Run tests: `python scripts/test_phase4.py`

### Limitations

- Single worker assumed (no concurrent processing)
- No automatic retry framework (manual status reset)
- Event ordering: FIFO by `event_at`, but concurrent transactions may interleave
- Hard deletes: Silver row physically removed; Bronze preserves full history
- Gold layer not updated in this phase

## Phase 5: Full Load, Incremental & Gold Orchestration

### Workflows

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `articles_full_load` | Manual | Initial historical load from PostgreSQL to DuckDB |
| `articles_incremental_load` | Hourly | Process queued events from PostgreSQL to DuckDB |

### Initial Full Load (`scripts/full_load.py`)

- Reads all articles from PostgreSQL using keyset pagination (by `id`, chunk size 1000)
- Populates `bronze.articles_snapshot` with `is_initial_snapshot = TRUE`
- Initializes `silver.articles_current` with active articles only
- Uses explicit column mappings and type conversions
- Tracks progress in `metadata.pipeline_state` (`FULL_LOAD_RUNNING` → `RECONCILING` → `READY`)
- After snapshot, replays captured events to reconcile
- Idempotent: refuses to run if snapshot already exists (prevents accidental duplicates)
- Pipeline marked `READY` only after full load and reconciliation succeed
- **Resumable**: On failure, cleans partial data (bronze/silver for IDs ≥ last_article_id) and resumes from `last_article_id`

### Incremental Load (`scripts/process_events.py`)

- Uses existing event processor functionality
- Processes QUEUED events in bounded batches (100 events/batch)
- Max runtime: ~45 minutes per run (configurable via `INCREMENTAL_MAX_RUNTIME`)
- Stops early if time runs low (< 30s remaining)
- Checks pipeline is `READY` before processing
- Recovers stale PROCESSING events (>5 min) back to QUEUED
- Preserves DuckDB checkpoint and PostgreSQL event status behavior
- Single worker: no concurrent DuckDB writers

### Gold Transformation (`scripts/run_gold.py` + `sql/10_gold_transformation.sql`)

- Computes daily metrics from Bronze snapshot + events
- Metrics: `articles_created`, `articles_published`, `articles_deleted` per UTC date
- Uses `created_at` for creation, `published_at` for publication, `event_at` for deletion
- Idempotent: `INSERT ... ON CONFLICT (metric_date) DO UPDATE`
- Source: Bronze snapshot (initial) + Bronze events (incremental)
- Limitations documented: pre-snapshot deletes unknown; historical publications only from captured data

### Data Quality Checks (`scripts/quality_checks.py`)

- No duplicate IDs in Silver
- Silver contains only active articles (no `deleted_at`)
- Gold metrics non-negative
- `metric_date` unique in Gold
- Gold has primary key on `metric_date` (idempotent upsert)
- Pipeline state valid (`READY` after full load)
- Processed events checkpoint consistent

### Validation Results

| Check | Result |
|-------|--------|
| Full load articles | 10,999 (matches PostgreSQL) |
| Bronze snapshot | 10,999 |
| Silver current | 10,999 (active only) |
| Gold metric dates | 3,990 |
| Pipeline state | READY |
| Incremental INSERT | ✓ Silver upsert |
| Incremental UPDATE | ✓ Silver update |
| Incremental DELETE | ✓ Silver hard delete |
| Gold idempotency | ✓ Re-run produces same results |
| Quality checks | ✓ All 7 checks pass |

### Commands

```bash
# Initial full load (run once)
python scripts/full_load.py

# Gold transformation
python scripts/run_gold.py

# Incremental load (hourly via Airflow or manual)
python scripts/process_events.py

# Quality checks
python scripts/quality_checks.py
```

## Phase 6: Airflow Reliability and Final Validation

### Key Improvements

| Area | Implementation |
|------|----------------|
| **Cross-DAG Locking** | `scripts/duckdb_lock.py` - file-based flock() on DuckDB file |
| **Full Load Resumable** | Progress tracking in `metadata.pipeline_state` (loaded_count, last_article_id, snapshot_batch_id) |
| **Failed Event Handling** | Max retries (3), stale FAILED event recovery, no infinite loops |
| **Runtime Limit** | Incremental load stops at 45 min, early stop at <30s remaining |
| **Gold After Upstream** | DAG dependencies: `incremental_load` → `gold_transform` |
| **Cross-DAG Concurrency** | File locking on DuckDB file via `duckdb_lock` context manager |
| **DAG Tests** | `scripts/test_dags.py` - importability, dependencies, timeouts, retries |

### DAGs (`dags/`)

- `articles_full_load.py` - Manual trigger, full load → gold → verify
- `articles_incremental_load.py` - Hourly schedule, incremental → gold

### Bugs Fixed in Phase 6

| # | Bug | Fix |
|---|-----|-----|
| 1 | Full load duplicate key on resume | Cleanup partial data (bronze/silver for IDs ≥ last_article_id) before resuming |
| 2 | Metadata schema DO \$\$ syntax | Removed PostgreSQL-specific DO \$\$ block; columns in CREATE TABLE |
| 3 | Full load reconciliation None duck_conn | Pass duck_conn to process_batch during reconciliation |
| 4 | Pipeline state columns missing | Added snapshot_batch_id, loaded_count, last_article_id |
| 5 | Full load duplicate key on silver | Proper reset/cleanup before fresh runs |
| 6 | DuckDB metadata DO \$\$ syntax error | Removed PostgreSQL-specific DO \$\$ block |
| 7 | Process_events lock issue | Incremental DAG uses file lock via duckdb_lock |
| 8 | Failed events infinite retry | max_retries (3) + stale FAILED event recovery |

## Common Errors & Fixes

| Error | Solution |
|-------|----------|
| `docker-compose: command not found` | Install Docker Compose plugin: `sudo apt-get install docker-compose-plugin` |
| `Connection refused` to PostgreSQL | Wait for health check to pass, check `docker-compose logs postgres` |
| `psycopg` import error | Ensure virtual environment is activated and dependencies installed |
| `duckdb` import error | Same as above |
| `.env` not found | Run `cp .env.example .env` |
| Port 5432 already in use | Change `POSTGRES_PORT` in `.env` or stop conflicting service |
| Port 5050 already in use | Change `PGADMIN_PORT` in `.env` or stop conflicting service |
| pgAdmin: "email not valid" | Use valid email format in `PGADMIN_EMAIL` (e.g., user@domain.com) |
| Generator exits: "table already contains data" | Run `TRUNCATE TABLE articles RESTART IDENTITY;` first |
| Warehouse init: "uuid extension not found" | DuckDB 1.5+ has built-in uuid() - no extension needed |
| Event processor: "pytz not found" | Install pytz: `pip install pytz` |
| Event processor: "no module named scripts" | Run from project root: `cd ~/workspace/kumparan_etl` |
| Trigger not firing | Check trigger exists: `\d articles` in psql |
| Events stuck in PROCESSING | Wait 5 min for auto-release or manually reset status |

## Cleanup

### Stop services (preserves data)

```bash
docker-compose down
```

### Stop services and remove volumes (deletes data)

```bash
docker-compose down -v
```

### Remove Python virtual environment

```bash
rm -rf .venv
```

### Remove generated data

```bash
rm -rf data/*.duckdb data/*.db
```

### Reset articles table (for re-generation)

```bash
docker-compose exec -T postgres psql -U kumparan -d kumparan -c "TRUNCATE TABLE articles RESTART IDENTITY;"
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `kumparan` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `kumparan` | PostgreSQL password |
| `POSTGRES_DB` | `kumparan` | PostgreSQL database name |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL |
| `PGADMIN_EMAIL` | `admin@kumparan.com` | pgAdmin login email |
| `PGADMIN_PASSWORD` | `admin` | pgAdmin login password |
| `PGADMIN_PORT` | `5050` | Host port for pgAdmin |
| `DUCKDB_PATH` | `data/warehouse.duckdb` | DuckDB database file path |

## Phase 1 Definition of Done

- [x] Project directory exists in Linux filesystem
- [x] Docker and Docker Compose verified
- [x] PostgreSQL starts successfully and passes health check
- [x] PostgreSQL data persists across container restart
- [x] Python virtual environment created and usable
- [x] Required dependencies can be imported
- [x] Python connects to PostgreSQL and executes query
- [x] Python creates and queries local DuckDB database
- [x] `.env.example` and `.gitignore` present and correct
- [x] README documents prerequisites, setup, verification, common errors, cleanup
- [x] No actual ETL pipeline implemented prematurely
- [x] No existing user data or files deleted

## Phase 2 Definition of Done

- [x] Articles schema created successfully
- [x] Dummy data generator runs successfully
- [x] Initial dataset: ~11,000 articles covering 2016-2026
- [x] Data validation queries pass
- [x] Re-running generator rejects duplicate data
- [x] Controlled test scenarios documented (not auto-executed)
- [x] PostgreSQL only database modified
- [x] No triggers, event log, ETL, or Airflow implemented
- [x] README documents setup, generation, validation, expected results

## Phase 3 Definition of Done

- [x] DuckDB warehouse initializes locally
- [x] All required schemas and tables exist with documented types and purposes
- [x] Initialization is repeatable and does not destroy existing data
- [x] Validation tests pass
- [x] README explains architecture and how to run checks
- [x] PostgreSQL data remains intact
- [x] Implementation is understandable and explainable

## Phase 4 Definition of Done

- [x] PostgreSQL `article_event` table created with UUID, JSONB payload, status tracking
- [x] Trigger captures INSERT, UPDATE, DELETE on `articles` in same transaction
- [x] No-op UPDATEs don't generate events
- [x] DELETE events preserve OLD row values
- [x] Python event processor reads QUEUED events, applies to DuckDB
- [x] Bronze event history preserved
- [x] Silver: upsert INSERT/UPDATE, hard delete on DELETE
- [x] Idempotent: checkpoint in `metadata.processed_events` prevents duplicates
- [x] Single DuckDB transaction for Bronze + Silver + checkpoint
- [x] Stale PROCESSING events released for retry
- [x] Failed events retriable via status reset
- [x] All 11 integration tests pass
- [x] PostgreSQL source data unchanged

## Phase 5 Definition of Done

- [x] Initial full load: keyset pagination, chunked inserts, progress tracking
- [x] Full load populates Bronze snapshot and Silver current
- [x] Post-snapshot event replay for reconciliation
- [x] Pipeline state tracked: INITIALIZING → FULL_LOAD_RUNNING → RECONCILING → READY
- [x] Incremental load uses existing event processor
- [x] Bounded batches (100), max runtime ~45 min, early stop on time
- [x] Stale event recovery (5 min timeout)
- [x] Pipeline READY check before incremental processing
- [x] Gold transformation: daily metrics from Bronze snapshot + events
- [x] Idempotent upsert on metric_date
- [x] Metrics: created, published, deleted per UTC date
- [x] Quality checks: 7 checks all pass
- [x] No data loss or duplication in full/incremental loads

## Phase 6 Definition of Done

- [x] Cross-DAG DuckDB file locking mechanism implemented
- [x] Full load resumable with progress tracking in `metadata.pipeline_state`
- [x] Failed events handling: max retry limit, stale event recovery, no infinite loops
- [x] Incremental load respects runtime limit (~45 min) and stops early on time pressure
- [x] Gold transformation only runs after upstream task success (DAG dependencies)
- [x] Cross-DAG concurrency protection via file locking on DuckDB file
- [x] DAG importability and dependency tests created (`scripts/test_dags.py`)
- [x] All regression tests pass (Phase 4: 11/11, Phase 5: 6/6, Quality: 7/7)
- [x] Full load resumable: cleans partial data on resume, tracks progress per chunk

## Known Bugs Fixed in Phase 6

1. **Full load duplicate key on resume**: Fixed by cleaning partial data (bronze/silver) for IDs >= last_article_id before resuming
2. **Metadata schema DO $$ syntax**: Removed PostgreSQL-specific DO $$ block; columns now defined in CREATE TABLE
3. **Full load reconciliation None duck_conn**: Fixed by passing duck_conn to process_batch during reconciliation
4. **Pipeline state columns missing**: Added snapshot_batch_id, loaded_count, last_article_id columns to pipeline_state table
5. **Full load duplicate key on silver table**: Fixed by proper reset/cleanup before fresh runs
6. **DuckDB metadata schema DO $$ syntax error**: Removed PostgreSQL-specific DO $$ block
7. **Process_events lock issue**: Updated incremental DAG to use file lock via duckdb_lock context manager
8. **Failed events infinite retry**: Added max_retries limit (default 3) and stale event recovery for FAILED events

## Remaining Limitations

- Single worker assumed (no concurrent processing)
- No automatic retry framework (manual status reset via SQL)
- Event ordering: FIFO by `event_at`, but concurrent transactions may interleave
- Hard deletes: Silver row physically removed; Bronze preserves full history
- Airflow DAGs not yet deployed (Phase 7+)
- DAG tests require Airflow installation (run in Airflow container)# kumparan-etl
