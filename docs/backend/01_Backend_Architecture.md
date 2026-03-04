# BE-01 — Backend: Architecture

## 1. Overview

The Backend is a **FastAPI** application (`backend/app/`) that serves as the central configuration API for the entire Rosetta platform. Its sole responsibility is managing the lifecycle of all configuration entities — sources, destinations, pipelines, schedules, tags, backfill jobs — while running a suite of background monitoring tasks in-process.

**Runtime characteristics:**

- **Synchronous SQLAlchemy 2.0** with `psycopg2` driver (`QueuePool`). There is **no** async ORM.
- **APScheduler** runs 9 periodic jobs in background threads sharing the process.
- **Redis** is used for caching (preview results, pipeline state).
- Optional **Celery dispatch**: when `WORKER_ENABLED=true`, heavy preview tasks are offloaded to the Worker service.

---

## 2. Layer Diagram (Clean Architecture)

```
┌───────────────────────────────────────────────────────┐
│  HTTP Client  (Web SPA / external tools)               │
└───────────────────────────┬───────────────────────────┘
                            │ HTTP
                            ▼
┌───────────────────────────────────────────────────────┐
│  API Layer  (app/api/v1/endpoints/*.py)                │
│  • 20 endpoint modules, all mounted under /api/v1      │
│  • Depends() injects services from app/api/deps.py     │
└───────────────────────────┬───────────────────────────┘
                            │ method calls
                            ▼
┌───────────────────────────────────────────────────────┐
│  Service Layer  (app/domain/services/*.py)             │
│  • Business logic, validation, orchestration           │
│  • PipelineService, SourceService, DestinationService  │
│  • BackfillService, TagService, PresetService, …       │
└───────────────────────────┬───────────────────────────┘
                            │ method calls
                            ▼
┌───────────────────────────────────────────────────────┐
│  Repository Layer  (app/domain/repositories/*.py)      │
│  • BaseRepository[ModelType] — generic CRUD            │
│  • Thin typed wrappers per model                       │
└───────────────────────────┬───────────────────────────┘
                            │ SQLAlchemy ORM
                            ▼
┌───────────────────────────────────────────────────────┐
│  Domain Model Layer  (app/domain/models/*.py)          │
│  • SQLAlchemy 2.0 Mapped[] + mapped_column()           │
│  • TimestampMixin (created_at, updated_at UTC+7)       │
└───────────────────────────┬───────────────────────────┘
                            │ psycopg2
                            ▼
                  PostgreSQL Config DB (:5433)
```

A dedicated **infrastructure** layer (`app/infrastructure/`) holds cross-cutting concerns:

- `tasks/scheduler.py` — APScheduler wrapper (9 jobs)
- `worker_client.py` — Celery task dispatcher
- `redis.py` — Redis connection helper
- `schema_cache.py` — In-process schema cache

---

## 3. Module Inventory

### 3.1 API Endpoints (`app/api/v1/endpoints/`)

| Module                 | Resource Domain                                              |
| ---------------------- | ------------------------------------------------------------ |
| `sources.py`           | Source configurations (PostgreSQL connections)               |
| `destinations.py`      | Destination configs (Snowflake, PG, Rosetta)                 |
| `pipelines.py`         | Pipeline CRUD + start/pause/refresh/preview                  |
| `table_sync.py`        | Per-table sync settings (custom SQL, filters)                |
| `backfill.py`          | Backfill job queue management                                |
| `schedules.py`         | Pipeline schedule CRUD                                       |
| `tags.py`              | Smart tag CRUD + pipeline assignments                        |
| `rosetta_chain.py`     | Chain config, client CRUD, key management                    |
| `configuration.py`     | Runtime key/value settings (`rosetta_setting_configuration`) |
| `dashboard.py`         | Dashboard aggregate stats                                    |
| `wal_monitor.py`       | WAL metric queries                                           |
| `wal_metrics.py`       | Historical WAL metric data                                   |
| `system_metrics.py`    | CPU/memory metric queries                                    |
| `job_metrics.py`       | Scheduler job execution timestamps                           |
| `credits.py`           | Snowflake credit usage                                       |
| `notification_logs.py` | Webhook/Telegram notification log                            |
| `flow_tasks.py`        | Flow task definitions                                        |
| `linked_tasks.py`      | Linked task config                                           |
| `schema_validation.py` | Schema evolution checks                                      |
| `health.py`            | `/health` liveness endpoint                                  |

### 3.2 Domain Models (`app/domain/models/`)

26 SQLAlchemy models. Key models:

| Model                          | Table                              |
| ------------------------------ | ---------------------------------- |
| `Pipeline`                     | `pipelines`                        |
| `PipelineMetadata`             | `pipeline_metadata`                |
| `PipelineDestination`          | `pipeline_destinations`            |
| `PipelineDestinationTableSync` | `pipelines_destination_table_sync` |
| `Source`                       | `sources`                          |
| `Destination`                  | `destinations`                     |
| `QueueBackfillData`            | `queue_backfill_data`              |
| `RosettaChainConfig`           | `rosetta_chain_config`             |
| `RosettaChainClient`           | `rosetta_chain_clients`            |
| `Tag`                          | `tbltag_list`                      |

All models inherit `Base` + `TimestampMixin`. The mixin adds `created_at` and `updated_at` columns auto-set to Asia/Jakarta (UTC+7).

### 3.3 Background Scheduler Jobs

9 APScheduler `IntervalTrigger` jobs registered at startup:

| Job key                    | Interval | Service Called                    |
| -------------------------- | -------- | --------------------------------- |
| `wal_monitor`              | 60 s     | `WALMonitorService`               |
| `replication_monitor`      | 60 s     | `ReplicationMonitorService`       |
| `schema_monitor`           | 60 s     | `SchemaMonitorService`            |
| `credit_monitor`           | 3600 s   | `CreditMonitorService`            |
| `table_list_refresh`       | 300 s    | `SourceService`                   |
| `system_metric_collection` | 5 s      | `SystemMetricService`             |
| `notification_sender`      | 30 s     | `NotificationService`             |
| `worker_health_check`      | 10 s     | HTTP GET to Worker `:8002/health` |
| `pipeline_refresh_check`   | 10 s     | `PipelineService`                 |

All jobs share a **persistent async event loop** running in a dedicated daemon thread (avoids `asyncio.run()` overhead per invocation). A **persistent `httpx.Client`** is kept alive for the worker health check job to avoid TCP connection churn.

---

## 4. Design Patterns

| Pattern                | Where Applied                                                       |
| ---------------------- | ------------------------------------------------------------------- |
| Repository Pattern     | `BaseRepository[T]` in `app/domain/repositories/base.py`            |
| Service Layer          | `app/domain/services/*.py` — all business logic lives here          |
| Dependency Injection   | FastAPI `Depends()` wires services via `app/api/deps.py`            |
| Factory (cached)       | `get_settings()` uses `@lru_cache` — one Settings instance          |
| Strategy               | Services call `encrypt_value`/`decrypt_value` — cipher is swappable |
| Observer (APScheduler) | Scheduler fires monitor jobs on interval triggers                   |

---

## 5. Inter-Service Interactions

```
Backend ←─── reads/writes ───▶ Config DB (5433)
Backend ──── Celery RPUSH ───▶ Redis (db 1) ──▶ Worker
Backend ──── cache GET/SET ─▶  Redis (db 0)
Backend ──── HTTP GET ───────▶ Worker :8002/health  (APScheduler job)
```

The Backend does **not** communicate directly with Compute. Compute reads from the same Config DB autonomously (polling every 10 s).

---

## 6. Application Startup Sequence

```
1. setup_logging()
2. get_settings()  →  validate all env vars, fail fast if missing required
3. db_manager.initialize()  →  create SQLAlchemy engine + session factory
4. background_scheduler.start()  →  register and fire all 9 APScheduler jobs
5. asyncio background task: check_database_health()
6. FastAPI begins accepting requests
```

On shutdown (SIGTERM/SIGINT):

```
1. background_scheduler.stop()  →  gracefully drain running jobs
2. db_manager.dispose()  →  close all pooled connections
3. httpx_client.close()
```
