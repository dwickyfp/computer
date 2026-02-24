# Rosetta Worker Service

Celery-based background task processor for heavy operations like preview SQL execution.

## Architecture

```
worker/
├── app/
│   ├── celery_app.py          # Celery instance & configuration
│   ├── config/
│   │   └── settings.py        # Pydantic settings (env vars)
│   ├── core/
│   │   ├── database.py        # SQLAlchemy session management
│   │   ├── security.py        # AES-256-GCM credential decryption
│   │   ├── redis_client.py    # Redis singleton for caching
│   │   ├── exceptions.py      # Worker-specific exceptions
│   │   └── logging.py         # Structured logging setup
│   ├── tasks/
│   │   ├── base.py            # BaseTask with lifecycle hooks
│   │   └── preview/
│   │       ├── task.py         # Celery task definition
│   │       ├── executor.py     # DuckDB query execution engine
│   │       ├── validator.py    # SQL safety validation
│   │       └── serializer.py   # Arrow → JSON serialization
│   └── services/
│       └── health_service.py   # Health check & worker stats
├── main.py                     # Entry point
├── start.sh                    # Startup script (Linux/Mac)
├── start.ps1                   # Startup script (Windows)
├── Dockerfile                  # Container image
├── pyproject.toml              # Dependencies (uv)
└── .env.example                # Environment template
```

## How It Works

1. **Backend** receives a preview request at `POST /pipelines/{id}/preview`
2. When `WORKER_ENABLED=true`, backend dispatches a Celery task via `WorkerClient`
3. Backend returns `{ task_id, state: "PENDING" }` immediately
4. **Worker** picks up the task from Redis broker (db 1)
5. Worker executes DuckDB query with Postgres extension (same logic as backend)
6. Frontend polls `GET /pipelines/{id}/preview/{task_id}` until complete
7. Results are cached in Redis (db 0) for 5 minutes

## Quick Start

### Prerequisites

- Python 3.12+
- Redis running on port 6379
- PostgreSQL config database on port 5433 (via docker-compose)

### Setup

```bash
cd worker

# Install dependencies
uv sync

# Copy and configure environment
cp .env.example .env
# Edit .env with your CREDENTIAL_ENCRYPTION_KEY (must match backend)
```

### Run Worker

**Linux/Mac:**

```bash
# Using start script
./start.sh

# With custom options
./start.sh --concurrency 8 --loglevel debug --beat
```

**Windows (PowerShell):**

```powershell
# Using PowerShell script
.\start.ps1

# With custom options
.\start.ps1 -Concurrency 8 -LogLevel debug -Beat
```

**Direct Celery (all platforms):**

```bash
# Basic worker
celery -A main worker --loglevel=info -Q preview,default -c 4 --pool=threads

# With Flower monitoring (optional)
celery -A main flower --port=5555
```

### Enable in Backend

Set these environment variables in the backend's `.env`:

```env
WORKER_ENABLED=true
CELERY_BROKER_URL=redis://localhost:6379/1
CELERY_RESULT_BACKEND=redis://localhost:6379/2
```

## Configuration

| Variable                      | Default                    | Description                      |
| ----------------------------- | -------------------------- | -------------------------------- |
| `DATABASE_URL`                | `postgresql://...`         | Config database URL              |
| `REDIS_URL`                   | `redis://localhost:6379/0` | Redis cache URL                  |
| `CELERY_BROKER_URL`           | `redis://localhost:6379/1` | Celery broker (Redis db 1)       |
| `CELERY_RESULT_BACKEND`       | `redis://localhost:6379/2` | Task result storage (Redis db 2) |
| `CREDENTIAL_ENCRYPTION_KEY`   | (required)                 | AES-256-GCM key (match backend)  |
| `WORKER_CONCURRENCY`          | `4`                        | Worker process count             |
| `WORKER_TASK_SOFT_TIME_LIMIT` | `120`                      | Soft timeout (seconds)           |
| `WORKER_TASK_HARD_TIME_LIMIT` | `180`                      | Hard timeout (seconds)           |
| `WORKER_PREVIEW_ROW_LIMIT`    | `100`                      | Max preview rows                 |
| `WORKER_DUCKDB_MEMORY_LIMIT`  | `1GB`                      | DuckDB memory limit              |
| `LOG_LEVEL`                   | `INFO`                     | Logging level                    |

## Docker

```bash
# Build and run with docker-compose
docker compose -f docker-compose-app.yml up -d rosetta-worker

# View logs
docker compose -f docker-compose-app.yml logs -f rosetta-worker
```

## Task Flow

```
Frontend ──POST──► Backend API
                    │
                    ├── WORKER_ENABLED=false → Execute sync → Return data
                    │
                    └── WORKER_ENABLED=true
                         │
                         ├── Submit to Celery → Return { task_id }
                         │
Frontend ──GET poll──► Backend API
                         │
                         └── Check AsyncResult → Return status/result
                              │
                         Worker Process
                              ├── Validate SQL
                              ├── Fetch source/dest configs
                              ├── Build DuckDB query (CTE + filter)
                              ├── Execute via Postgres extension
                              ├── Serialize Arrow → JSON
                              └── Cache in Redis (5min TTL)
```
