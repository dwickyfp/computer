# Rosetta ETL Platform - AI Agent Instructions

## Project Overview

Rosetta is a real-time ETL platform with a **four-service architecture**:

- **Backend** (FastAPI/Python): Configuration API — Clean Architecture with DDD, sync SQLAlchemy 2.0, APScheduler background jobs
- **Compute** (Python/Debezium): CDC engine — process-isolated pipelines replicating PostgreSQL → Snowflake/PostgreSQL
- **Worker** (Celery/Python): Background task processor — DuckDB-based SQL preview execution with thread pool
- **Web** (React/TypeScript/Vite): Admin dashboard — TanStack ecosystem (Router/Query/Table), shadcn/ui

## Architecture Patterns

### Backend: Clean Architecture (`backend/app/`)

```
api/v1/endpoints/          # FastAPI routes (17 endpoint modules)
domain/services/           # Business logic (inject via app.api.deps)
domain/models/             # SQLAlchemy 2.0 ORM (Mapped[] + mapped_column)
domain/schemas/            # Pydantic v1 validation
domain/repositories/       # BaseRepository[ModelType] generic CRUD
infrastructure/tasks/      # APScheduler (9 periodic jobs)
infrastructure/worker_client.py  # Celery task dispatcher (when worker_enabled=true)
core/config.py             # Pydantic BaseSettings with @lru_cache
core/database.py           # Sync Session (psycopg2), NOT async
core/security.py           # AES-256-GCM encrypt_value()/decrypt_value()
```

**Critical conventions:**

- **Sync SQLAlchemy** — uses `Session` (not `AsyncSession`), `psycopg2` driver, `QueuePool`, `expire_on_commit=False`
- Never access ORM in endpoints — always go through services from `app.api.deps` (`Depends(get_pipeline_service)` etc.)
- 6 injected services: `SourceService`, `DestinationService`, `PipelineService`, `PresetService`, `BackfillService`, `TagService`
- `BaseRepository` provides: `create`, `get_by_id`, `get_by_name`, `get_all`, `count`, `update`, `delete`, `exists`
- Repository `update()` auto-sets `updated_at` to Asia/Jakarta timezone
- Models inherit `Base` + `TimestampMixin`, use `lazy="selectin"` for eager loading
- Pipeline creation forces `status='PAUSE'` — must call `/start` endpoint explicitly
- Status changes must update both `Pipeline.status` and `PipelineMetadata.status`

### Backend Background Scheduler (APScheduler)

9 jobs run via `BackgroundScheduler` at startup (`infrastructure/tasks/scheduler.py`):

| Job | Interval | Purpose |
|-----|----------|---------|
| `wal_monitor` | 60s (configurable) | WAL size monitoring |
| `replication_monitor` | 60s | Source replication status |
| `schema_monitor` | 60s | Schema change detection |
| `credit_monitor` | 1 hour | Snowflake credit usage |
| `table_list_refresh` | 5 min | Auto-refresh available tables |
| `system_metric_collection` | 5s | CPU/memory metrics |
| `notification_sender` | 30s | Webhook/Telegram notifications |
| `worker_health_check` | 10s | Poll worker health (if `worker_enabled`) |
| `pipeline_refresh_check` | 10s | Auto-refresh flagged pipelines |

### Web: Feature-Based Organization (`web/src/`)

```
features/<feature>/components/  # Feature UI (pipelines has 27+ components)
features/<feature>/pages/       # Route pages
features/<feature>/data/        # Zod schemas & table column configs
repo/                           # API layer (13 files) — always use `api` from repo/client.ts
components/ui/                  # shadcn/ui primitives
```

**Critical conventions:**

- API client: `import { api } from '@/repo/client'` — never use raw axios
- Base URL: `VITE_API_URL` → dev `localhost:8000/api/v1` → prod `window.location.origin/api`
- Mutations: add **300ms delay** before `queryClient.invalidateQueries()` for DB commit timing
- Tables: generic `DataTableProps<TData, TValue>` + `@tanstack/react-table` + shared `DataTableToolbar`/`DataTablePagination`
- Forms: `react-hook-form` + `zod` + `@hookform/resolvers`
- Routes auto-generated in `src/routeTree.gen.ts` by TanStack Router plugin

### Compute: CDC Engine (`compute/`)

- `PipelineManager` polls DB every 10s, spawns `multiprocessing.Process` per pipeline via `PipelineEngine`
- Each process gets its own connection pool (`PIPELINE_POOL_MAX_CONN`, default 20)
- Destinations: `PostgreSQLDestination` (DuckDB `MERGE INTO`), `SnowflakeDestination` (Snowpipe Streaming REST + JWT)
- `CDCRecord` operations: `c`=create, `u`=update, `d`=delete, `r`=read/snapshot
- `BackfillManager` polls `queue_backfill_data` every 5s, runs in separate threads
- DLQ: Redis Streams per table/destination — `dlq:{source_id}:{table}:{dest_id}`
- Runs `migrations/001_create_table.sql` on startup

### Worker: Celery Task Processor (`worker/`)

```
worker/app/celery_app.py       # Celery factory (Redis broker db 1, results db 2)
worker/app/tasks/preview/      # DuckDB query execution — task name: "worker.preview.execute"
worker/app/core/security.py    # AES-256-GCM (CREDENTIAL_ENCRYPTION_KEY must match backend)
worker/server.py               # FastAPI health API on port 8002
worker/start.sh                # Starts health API + celery worker (--pool=threads)
```

- **Sync SQLAlchemy** + **thread pool** (`--pool=threads` avoids DuckDB fork crashes)
- `task_acks_late=True` for crash safety, preview results cached in Redis (5min TTL)
- Backend conditionally dispatches: `WORKER_ENABLED=true` → `WorkerClient.submit_preview_task()` → Celery; `false` → sync in-process

## Development Workflows

### Backend

```bash
cd backend && uv sync && uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
uv run pytest tests/ --cov=app    # Tests use sync fixtures (not async)
```

Migrations: `uv run alembic revision --autogenerate -m "description"` → `uv run alembic upgrade head`

### Web

```bash
cd web && pnpm install && pnpm dev    # Port 5173
pnpm build && pnpm lint && pnpm format
```

### Compute

```bash
cd compute && pip install -r requirements.txt
CONFIG_DATABASE_URL=... python main.py    # Port 8001 (health API)
```

### Worker

```bash
cd worker && uv sync && cp .env.example .env
./start.sh    # Starts health API (8002) + Celery worker
# Or: celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads
```

### Docker Infrastructure

```bash
docker-compose up -d    # 5 containers: config DB (5433), source DB (5434), target DBs (5435, 5436), Redis (6379)
```

## Port Reference

| Service | Port | Notes |
|---------|------|-------|
| Backend | 8000 | FastAPI + APScheduler |
| Compute | 8001 | CDC engine + health API |
| Worker | 8002 | Celery + health API |
| Web | 5173 | Vite dev server |
| Config DB | 5433 | Shared PostgreSQL (`wal_level=logical`) |
| Source DB | 5434 | PostGIS CDC source |
| Target DB 1 | 5435 | PostgreSQL destination |
| Target DB 2 | 5436 | PostgreSQL destination |
| Redis | 6379 | db0=cache, db1=Celery broker, db2=Celery results |

## Shared Config Database

All services share PostgreSQL config tables (schema in `migrations/001_create_table.sql`). Key tables:

- `pipelines`, `pipeline_destinations`, `pipeline_metadata` — pipeline config + per-destination health
- `pipelines_destination_table_sync` — per-table sync config (custom SQL, filters, Snowflake object tracking)
- `sources`, `destinations` — connection configs (credentials encrypted via AES-256-GCM)
- `queue_backfill_data` — backfill job queue with resumable state (`last_pk_value`)
- `rosetta_setting_configuration` — key/value runtime config (WAL thresholds, batch sizes)
- `tbltag_list` + `pipelines_destination_table_sync_tag` — smart tag M2M
- `worker_health_status` — Celery worker health snapshots

## Security

- `CREDENTIAL_ENCRYPTION_KEY` — AES-256-GCM shared secret between Backend and Worker (must match)
- Format: `base64(12-byte nonce || ciphertext)` — see `backend/app/core/security.py`
- Snowflake uses RSA key-pair auth (PKCS#8 encrypted private keys, not passwords)

## Common Pitfalls

1. **Backend DB is sync** — don't use `async/await` or `AsyncSession` in backend code
2. **Backend**: Always use service injection from `app.api.deps` — never bypass to repositories directly from endpoints
3. **Web**: Must add 300ms delay before `invalidateQueries` — without it, UI shows stale data
4. **Web**: Always `import { api } from '@/repo/client'` — direct axios imports break base URL resolution
5. **Worker**: `CREDENTIAL_ENCRYPTION_KEY` must be identical to backend's — mismatch causes silent decrypt failures
6. **Compute**: Config changes need Compute restart (polls DB, doesn't receive push notifications)
7. **Compute**: Snowflake destination uses Snowpipe Streaming REST API (not Snowflake connector) — see `compute/destinations/snowflake/`
8. **Compute**: PostgreSQL destination replication uses DuckDB `MERGE INTO` — not direct SQL inserts

## Key Dependencies

- **Backend**: FastAPI, SQLAlchemy 2.0 (sync), Pydantic v1, psycopg2, Alembic, APScheduler
- **Compute**: pydbzengine (Debezium), psycopg2-binary, httpx, DuckDB
- **Worker**: Celery[redis], DuckDB, PyArrow, structlog
- **Web**: React 19, TanStack Router/Query/Table, shadcn/ui, Zod, axios
