# WK-01 — Worker: Architecture

## 1. Overview

The Worker service is a **Celery-based background task processor** that executes computationally expensive operations outside the Backend request cycle. Its primary job is running **SQL preview queries** against live source databases using **DuckDB** — returning column metadata, sample rows, and optional data profiling statistics.

The Worker is **optional** — when `WORKER_ENABLED=false` (default), the Backend executes previews synchronously in-process. When enabled, offloading to the Worker prevents slow preview queries from blocking Backend API threads.

**Key characteristics:**

- Celery with **thread pool** (`--pool=threads`): DuckDB cannot survive a `fork()`, so thread-based concurrency is mandatory.
- Shares `CREDENTIAL_ENCRYPTION_KEY` with Backend — must be identical to decrypt source/destination credentials.
- Results cached in Redis with a 5-minute TTL.
- Exposes a lightweight **FastAPI health API** on `:8002`.

---

## 2. Architecture Diagram

```
Backend  ──Redis RPUSH──▶  Redis Broker (db 1)
                                  │
                          ┌───────▼────────────────────────────────┐
                          │  Celery Worker Process                  │
                          │                                         │
                          │  --pool=threads  (N worker threads)     │
                          │                                         │
                          │  ┌──────────────────────────────────┐  │
                          │  │  Thread: worker.preview.execute   │  │
                          │  │    1. Decrypt source credentials  │  │
                          │  │    2. DuckDB attach PostgreSQL     │  │
                          │  │    3. Execute SQL                  │  │
                          │  │    4. Serialise results            │  │
                          │  │    5. Cache in Redis (5 min TTL)   │  │
                          │  └──────────────────────────────────┘  │
                          │                                         │
                          │  ┌──────────────────────────────────┐  │
                          │  │  Thread: worker.flow_task.preview │  │
                          │  │  Thread: worker.linked_task.execute│ │
                          │  └──────────────────────────────────┘  │
                          └─────────────────────────────────────────┘
                                  │
                          Redis Results (db 2)  ◀─── Backend polls
```

---

## 3. Module Inventory

```
worker/
├── main.py              # Entry point (imports celery_app for worker CLI)
├── server.py            # FastAPI health API (port 8002)
├── start.sh             # Starts server.py + Celery worker
├── start.ps1            # Windows equivalent of start.sh
└── app/
    ├── celery_app.py    # Celery factory + full configuration
    ├── config/
    │   └── settings.py  # Pydantic BaseSettings for Worker
    ├── core/
    │   ├── logging.py   # structlog setup
    │   ├── security.py  # AES-256-GCM (mirrors backend)
    │   └── database.py  # psycopg2 for reading source configs
    ├── services/
    │   └── source_service.py  # Fetches + decrypts source config
    └── tasks/
        ├── base.py            # BaseTask (common error handling)
        ├── preview/
        │   ├── task.py        # @celery_app.task: worker.preview.execute
        │   ├── executor.py    # DuckDB query execution logic
        │   ├── validator.py   # SQL safety checks
        │   ├── serializer.py  # Result → JSON-serialisable dict
        │   └── profiler.py    # Column profiling statistics (optional)
        ├── flow_task/
        │   └── task.py        # worker.flow_task.preview
        ├── linked_task/
        │   └── task.py        # worker.linked_task.execute
        ├── lineage/
        │   └── task.py        # SQL lineage extraction task
        └── destination_table_list/
            └── task.py        # List tables from destination
```

---

## 4. Task Registry

| Celery Task Name               | Queue           | Purpose                                     |
| ------------------------------ | --------------- | ------------------------------------------- |
| `worker.preview.execute`       | `preview`       | DuckDB SQL preview for a source/destination |
| `worker.flow_task.preview`     | `preview`       | SQL preview within a Flow Task context      |
| `worker.linked_task.execute`   | `orchestration` | Execute a linked task pipeline              |
| (lineage tasks)                | `default`       | Lineage graph extraction                    |
| (destination_table_list tasks) | `default`       | Enumerate tables in a destination           |

Default queue for unrouted tasks: `default`.

---

## 5. Celery Configuration Highlights

| Setting                           | Value     | Rationale                                                     |
| --------------------------------- | --------- | ------------------------------------------------------------- |
| `task_acks_late=True`             | global    | Task acked only after completion — safe on worker crash       |
| `task_reject_on_worker_lost=True` | global    | Re-queue if worker dies mid-task (stateless tasks only)       |
| `worker_prefetch_multiplier=1`    | global    | DuckDB tasks are CPU/memory intensive — avoid over-fetching   |
| `--pool=threads`                  | CLI flag  | DuckDB cannot fork; threads share same process safely         |
| `result_expires=600`              | 10 min    | Results auto-expire from Redis to prevent memory accumulation |
| `broker_pool_limit=10`            | global    | Redis connection pool (1 per worker thread + headroom)        |
| `visibility_timeout=3600`         | transport | 1 h — allows long-running tasks without re-delivery           |

---

## 6. Preview Result Caching

Preview results are cached in Redis with a 5-minute TTL:

```
Cache key: preview:{hash}
    where hash = sha256(sql + source_id + destination_id + table_name + filter_sql)

Cache HIT:  Backend short-circuits, returns cached result immediately
Cache MISS: Dispatch Celery task → execute → store result → return to Backend

TTL: 300 seconds (5 minutes)
```

This prevents redundant DuckDB queries when the same preview is requested multiple times within a short window.

---

## 7. Health API (`server.py`)

A minimal FastAPI app runs on `:8002` alongside the Celery worker:

```
GET /health
Response: {"status": "healthy", "worker_concurrency": 4, "active_tasks": 2}

GET /health/tasks
Response: list of active task IDs and their states
```

The Backend APScheduler job polls `GET :8002/health` every 10 s and records the result in `worker_health_status` table.

---

## 8. Design Patterns

| Pattern         | Where Applied                                               |
| --------------- | ----------------------------------------------------------- |
| Task Base Class | `BaseTask` provides unified `on_failure` / `on_retry` hooks |
| Decorator       | `@celery_app.task` registers functions as Celery tasks      |
| Strategy        | `executor.py` accepts a callable query strategy             |
| Singleton cache | `@lru_cache` on `get_settings()`                            |
