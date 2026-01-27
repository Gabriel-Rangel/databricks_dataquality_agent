# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important: Always Check Documentation

Before making changes, consult the detailed documentation in `docs/`:

| File | Content |
|------|---------|
| `docs/runbook.md` | Deployment guide, prerequisites, troubleshooting |
| `docs/authentication.md` | OBO auth, service principal, OAuth flows |
| `docs/architecture.md` | System design, component diagrams, data flow |
| `docs/configuration.md` | Environment variables, bundle variables, secrets |
| `docs/api-reference.md` | All REST API endpoints with request/response examples |
| `docs/dqx-checks.md` | Available DQX check functions reference |
| `docs/ci-cd.md` | GitHub Actions, OIDC federation setup |

## Project Overview

DQX Data Quality Manager - A Databricks App (Flask) for AI-powered data quality rule generation and validation using [Databricks DQX](https://databrickslabs.github.io/dqx/). Users select Unity Catalog tables, describe data quality requirements in natural language, and the app generates DQX-compatible rules via serverless jobs.

## Build & Development Commands

```bash
# Install dependencies (from project root)
pip install -r src/requirements.txt
pip install -e ".[dev]"

# Run Flask app locally (requires environment variables below)
cd src && python wsgi.py
# Access at http://localhost:8000

# Run all unit tests
pytest tests/unit -v --tb=short -m "not integration"

# Run a single test file
pytest tests/unit/services/test_databricks.py -v

# Run a specific test
pytest tests/unit/services/test_databricks.py::test_get_catalogs -v

# Lint check (syntax errors only)
flake8 src/app --count --select=E9,F63,F7,F82 --show-source --statistics

# Verify app imports work
cd src && python -c "from app import create_app; app = create_app(); print('OK')"

# Validate Databricks Asset Bundle
databricks bundle validate -t dev

# Deploy to Databricks
databricks bundle deploy -t dev

# Destroy deployment
databricks bundle destroy -t dev
```

## Required Environment Variables for Local Development

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
export DQ_GENERATION_JOB_ID="your-generation-job-id"
export DQ_VALIDATION_JOB_ID="your-validation-job-id"
export SQL_WAREHOUSE_ID="your-warehouse-id"

# Optional
export LAKEBASE_HOST="your-lakebase-host"
export MODEL_SERVING_ENDPOINT="databricks-claude-sonnet-4-5"  # default
```

## Architecture

### Authentication Model (Three-Layer)

| Component | Auth Method | Purpose |
|-----------|-------------|---------|
| Unity Catalog SQL | User Token (OBO via `x-forwarded-access-token`) | Data queries respect user permissions |
| AI Analysis | User Token (OBO) | Execute ai_query() as user |
| Databricks Jobs | App Service Principal | Trigger generation/validation jobs (no "jobs" scope available for user auth) |
| Lakebase (PostgreSQL) | User OAuth Token | Store rules with user identity |

The app falls back to `DATABRICKS_TOKEN` when no `x-forwarded-access-token` header is present (local development).

### Key Services (`src/app/services/`)

- **databricks.py**: `DatabricksService` - Unity Catalog operations (SQL via `databricks-sql-connector` with OBO), job triggering (via `WorkspaceClient` with SP credentials)
- **ai.py**: `AIAnalysisService` - Rules analysis using `ai_query()` via SQL Statement Execution API
- **lakebase.py**: `LakebaseService` - PostgreSQL storage for versioned DQ rules with OAuth authentication

### Request Flow

1. User selects table → `DatabricksService.execute_sql()` with user's OBO token
2. User submits prompt → `DatabricksService.trigger_dq_job()` with app SP credentials
3. Serverless notebook (`notebooks/generate_dq_rules_fast.py`) runs DQX profiler + AI generator using `databricks-labs-dqx[llm]`
4. Results returned → optionally analyzed by `AIAnalysisService`, saved to Lakebase

### Databricks Asset Bundle Structure

```
databricks.yml                    # Main bundle config, includes other files
resources/
├── apps.yml                      # App definition + resource bindings (sql, jobs)
├── generation_job.yml            # Serverless generation job
└── validation_job.yml            # Serverless validation job
environments/{dev,stage,prod}/
├── targets.yml                   # Target config (workspace root_path)
├── variables.yml                 # Environment-specific variables
└── permissions.yml               # Permissions
```

Key: Notebooks use `${workspace.root_path}/notebooks/...` path so app SP can access them.

### Flask Blueprints (`src/app/routes/`)

| Blueprint | Base Path | Purpose |
|-----------|-----------|---------|
| `catalog.py` | `/api/catalogs`, `/api/schemas`, `/api/tables`, `/api/sample` | Unity Catalog browsing (OBO) |
| `rules.py` | `/api/generate`, `/api/status`, `/api/validate`, `/api/analyze` | Rule generation, validation, AI analysis |
| `lakebase.py` | `/api/lakebase`, `/api/confirm`, `/api/history` | Rule persistence |

## Testing

Tests use `pytest-mock` for mocking. Key fixtures in `tests/conftest.py`:
- `app`, `client` - Flask test app and client
- `mock_databricks_service`, `mock_lakebase_service`, `mock_ai_service` - Pre-configured mocks
- `sample_rules` - Example DQX rule structure

Test markers: `@pytest.mark.unit`, `@pytest.mark.integration`

## DQX Rule Format

Rules follow this structure (used throughout the codebase):
```python
{
    "check": {
        "function": "is_not_null",  # DQX check function name
        "arguments": {"col_name": "column_name"}
    },
    "name": "rule_name",
    "criticality": "error"  # or "warn"
}
```

Common check functions: `is_not_null`, `is_not_empty`, `is_in_list`, `is_in_range`, `regex_match`, `is_unique`, `is_valid_date`, `is_valid_timestamp`, `is_valid_json`. Full reference: [DQX Quality Checks](https://databrickslabs.github.io/dqx/docs/reference/quality_checks/)

## Lakebase Schema

Rules stored in `dq_rules_events` table with versioning:
```sql
CREATE TABLE dq_rules_events (
    id UUID PRIMARY KEY,
    table_name VARCHAR(500) NOT NULL,
    version INTEGER NOT NULL,
    rules JSONB NOT NULL,
    user_prompt TEXT,
    ai_summary JSONB,
    created_at TIMESTAMP,
    created_by VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(table_name, version)
);
```

## CI/CD

| Environment | Trigger | App Name |
|-------------|---------|----------|
| `dev` | Push to main, PR | dqx-rule-generator-dev |
| `stage` | Manual | dqx-rule-generator-stage |
| `prod` | Manual | dqx-rule-generator |

Uses GitHub OIDC federation with Databricks Service Principal. Required secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `SQL_WAREHOUSE_ID`.

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| "No catalogs available" | Missing permissions or warehouse down | Check `USE CATALOG` permission, verify SQL Warehouse is running |
| "Job failed: Unable to access notebook" | Notebook path issue | Ensure using `${workspace.root_path}/notebooks/...` in variables.yml |
| "Lakebase connection failed" | Wrong host or not authenticated | Verify `LAKEBASE_HOST`, ensure user is logged in via Databricks Apps |

Debug: `curl <app-url>/api/debug` shows configuration status.
Logs: `databricks apps logs dqx-rule-generator-dev`
