# Phase 2 — Data Preparation
## Kumparan Data Engineer Assessment

### Objective

Prepare a realistic local PostgreSQL source dataset for the Kumparan Data Engineer assessment.

Phase 1 has been completed. PostgreSQL connectivity and DuckDB functionality have been verified.

Before making changes, inspect the existing repository and Phase 1 implementation. Preserve existing configuration and working verification scripts.

### Scope

Implement only the source schema, deterministic dummy-data generation, and data-validation checks.

Do not implement ETL, triggers, `article_event`, DuckDB Bronze/Silver/Gold schemas, or Airflow DAGs in this phase.

### 1. Source table

Create a SQL file under `sql/` that defines the PostgreSQL `articles` table with these fields:

- `id`
- `title`
- `content`
- `published_at`
- `author_id`
- `created_at`
- `updated_at`
- `deleted_at`

Requirements:

- `id` is the primary key.
- Use appropriate PostgreSQL types, including `TIMESTAMPTZ` for timestamps.
- `title` and `content` should support realistic article text.
- `author_id` should be a positive integer.
- `created_at` and `updated_at` must be populated.
- `updated_at` must not precede `created_at`.
- `published_at` may be NULL for unpublished articles.
- `deleted_at` is included to match the assessment schema, but this phase must not implement soft-delete behavior.
- Do not invent additional mandatory business fields.

Use a reproducible schema setup script. Avoid destructive operations such as dropping the database or deleting unrelated data.

### 2. Dummy-data generator

Create a Python script under `scripts/` to generate deterministic dummy data for 2016–2026.

Target approximately 11,000 initial articles:
- Approximately 10,000 articles created from 2016 through 2025.
- Approximately 1,000 articles created in 2026.

Requirements:

- Use a fixed random seed so repeated generation produces reproducible results.
- Generate varied titles and article content.
- Use a reasonable range of positive `author_id` values.
- Include published and unpublished articles.
- Include realistic relationships between `created_at`, `updated_at`, and `published_at`.
- Generate timezone-aware timestamps.
- Ensure `updated_at >= created_at`.
- Keep `deleted_at` NULL for the initial dataset.
- Use parameterized SQL or safe bulk insertion.
- Insert in manageable batches rather than issuing one INSERT per row.
- Make generation idempotent or explicitly reject accidental duplicate loading. Re-running the script must not silently create duplicate copies of the dataset.

The generator should accept configurable record counts or a clearly documented default.

Do not connect to or write data into DuckDB during this phase.

### 3. Validation SQL

Create SQL queries to validate:

- Total article count.
- Minimum and maximum `created_at`.
- Distribution of articles by creation year.
- Published versus unpublished counts.
- NULL counts for relevant fields.
- Invalid timestamp relationships.
- Duplicate primary keys, if applicable.

The checks should make it easy to confirm that the generated dataset meets expectations.

### 4. Controlled test scenarios

Prepare a small, separate script or documented SQL examples for later testing:

1. Insert a new article.
2. Update an existing article.
3. Hard-delete an existing article.
4. Insert an unpublished article.
5. Update an article's publication timestamp.

Use clearly identifiable test records so they can be found and cleaned up deliberately.

Do not execute destructive scenarios automatically during initial dataset generation.

### 5. Configuration and safety

Reuse the existing `.env` configuration and PostgreSQL connection helper, if present.

Do not hardcode credentials.

Do not overwrite existing data or drop tables automatically.

If the target table already contains data, inspect it and provide a safe, explicit choice to reuse, reject, or reset the dataset. Any reset must require deliberate user action.

### 6. Definition of done

Phase 2 is complete when:

1. The `articles` schema is created successfully.
2. The dummy-data generator runs successfully.
3. The initial dataset has the expected approximate size and covers 2016–2026.
4. Data validation queries pass.
5. Re-running the generator does not create duplicate data.
6. Controlled test scenarios are documented but not automatically executed.
7. PostgreSQL remains the only database modified by this phase.
8. No trigger, event log, ETL, or Airflow workflow has been implemented.
9. The README documents setup, data generation, validation, and expected results.

### Agent workflow

Inspect the repository first. Reuse existing utilities where appropriate, implement the smallest necessary changes, run the schema and data-generation scripts, execute the validation queries, and report actual results.

Do not claim that tests passed unless they were executed.

Do not proceed to Phase 3 without explicit user approval.