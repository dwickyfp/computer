# BE-05 — Backend: Deployment & Setup

## 1. Prerequisites

| Dependency     | Minimum Version | Notes                                                            |
| -------------- | --------------- | ---------------------------------------------------------------- |
| Python         | 3.11+           | Uses `zoneinfo`, `match` statements                              |
| uv             | 0.4+            | Package manager / virtual env                                    |
| PostgreSQL     | 14+             | Config DB must have `wal_level=logical`                          |
| Redis          | 6+              | Cache, optional Celery broker                                    |
| Java (JRE/JDK) | 11+             | Required by Debezium via pydbzengine (Compute) — **not** Backend |

---

## 2. Local Development Setup

```bash
# 1. Clone and enter backend directory
cd backend

# 2. Install dependencies (creates .venv automatically)
uv sync

# 3. Start infrastructure (PostgreSQL + Redis)
docker-compose -f docker-compose.yml up -d

# 4. Copy and configure environment
cp .env.example .env
# Edit .env — set DATABASE_URL, SECRET_KEY, CREDENTIAL_ENCRYPTION_KEY

# 5. Run database migrations
uv run alembic upgrade head

# 6. Start development server
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

---

## 3. Required Environment Variables

| Variable                       | Required | Default                    | Description                                            |
| ------------------------------ | -------- | -------------------------- | ------------------------------------------------------ |
| `DATABASE_URL`                 | ✅       | —                          | PostgreSQL DSN (`postgresql://user:pass@host:port/db`) |
| `SECRET_KEY`                   | ✅       | —                          | ≥ 32 chars; used for JWT signing                       |
| `CREDENTIAL_ENCRYPTION_KEY`    | ✅       | —                          | Exactly 32 bytes or base64-encoded 32 bytes            |
| `REDIS_URL`                    | ➖       | `redis://localhost:6379/0` | Redis connection URL                                   |
| `APP_ENV`                      | ➖       | `development`              | `development` / `staging` / `production`               |
| `LOG_LEVEL`                    | ➖       | `INFO`                     | `DEBUG` / `INFO` / `WARNING` / `ERROR`                 |
| `WORKER_ENABLED`               | ➖       | `false`                    | `true` to dispatch preview tasks to Celery Worker      |
| `CORS_ORIGINS`                 | ➖       | `["*"]`                    | JSON array of allowed CORS origins                     |
| `DB_POOL_SIZE`                 | ➖       | `5`                        | SQLAlchemy pool size (1–100)                           |
| `DB_MAX_OVERFLOW`              | ➖       | `5`                        | Max overflow beyond pool size (0–50)                   |
| `WAL_MONITOR_INTERVAL_SECONDS` | ➖       | `60`                       | WAL monitor poll interval (≥ 60)                       |

---

## 4. Database Migrations

Rosetta uses **Alembic** for schema management.

```bash
# Apply all pending migrations
uv run alembic upgrade head

# Create a new migration (auto-generate from model changes)
uv run alembic revision --autogenerate -m "add_new_column_to_pipelines"

# Downgrade one step
uv run alembic downgrade -1

# Check current revision
uv run alembic current

# Show migration history
uv run alembic history
```

> **Important:** The Compute service also runs `migrations/001_create_table.sql` on startup. Do not conflict these migration paths — Alembic (Backend) manages the schema; the raw SQL migration only bootstraps the shared tables that both Compute and Backend need.

---

## 5. Running Tests

```bash
# Run all tests with coverage
uv run pytest tests/ --cov=app --cov-report=term-missing

# Run a specific test file
uv run pytest tests/test_flow_task_versions.py -v

# Run with verbose output
uv run pytest tests/ -v

# Run with specific marker
uv run pytest tests/ -m "not slow"
```

Tests use **sync fixtures** only — the backend has no async paths.

---

## 6. Docker Deployment

### Single-container (production)

```bash
# Build
docker build -t rosetta-backend .

# Run
docker run -d \
  --name rosetta-backend \
  -p 8000:8000 \
  -e DATABASE_URL="postgresql://..." \
  -e SECRET_KEY="..." \
  -e CREDENTIAL_ENCRYPTION_KEY="..." \
  rosetta-backend
```

### Docker Compose (full stack)

```bash
# Development stack
docker-compose -f docker-compose-dev.yml up -d

# Production application stack
docker-compose -f docker-compose-app.yml up -d
```

The main `docker-compose-app.yml` uses **supervisord** to manage Backend + Compute + Worker in a single container with Nginx as reverse proxy (see `docker/supervisord.conf`).

---

## 7. Health & Observability

| Endpoint                     | Description                                      |
| ---------------------------- | ------------------------------------------------ |
| `GET /health`                | Liveness probe — returns `{"status": "healthy"}` |
| `GET /api/v1/job-metrics`    | Last execution time of each scheduler job        |
| `GET /api/v1/system-metrics` | CPU/memory time-series (last N minutes)          |
| `GET /api/v1/worker-health`  | Last known Worker health status                  |

All endpoints return structured JSON. The `/health` endpoint does not require auth.

---

## 8. Production Checklist

- [ ] `APP_ENV=production`
- [ ] `DEBUG=false`
- [ ] `LOG_LEVEL=WARNING` or `ERROR`
- [ ] `CORS_ORIGINS` restricted to actual frontend origin
- [ ] `SECRET_KEY` is random, ≥ 64 chars
- [ ] `CREDENTIAL_ENCRYPTION_KEY` is exactly 32 bytes, stored in a secrets manager
- [ ] `DB_POOL_SIZE` tuned for expected concurrency (recommend 10–20 for production)
- [ ] Redis persistence (`appendonly yes`) enabled for Celery result safety
- [ ] Nginx or load balancer terminates TLS — Backend should not handle TLS directly
- [ ] Alembic migrations applied before rolling new backend version
