# BE-02 — Backend: Application Flow

## 1. Request Lifecycle

Every HTTP request follows this path:

```
HTTP Client
    │
    ▼
FastAPI ASGI (Uvicorn)
    │
    ├─ CORS Middleware  (adds Access-Control-Allow-* headers)
    │
    ├─ Global Exception Handlers
    │     • RosettaException  → structured JSON error
    │     • RequestValidationError → 422 with field-level detail
    │     • unhandled Exception → sanitized 500 response
    │
    ▼
Router  /api/v1/<resource>/<action>
    │
    ├─ Depends(get_db)  →  opens SQLAlchemy Session from QueuePool
    ├─ Depends(get_<name>_service)  →  instantiates Service with db session
    │
    ▼
Endpoint Function
    │  validates Pydantic request body
    │  calls service method(s)
    │
    ▼
Service Layer
    │  applies business rules
    │  calls repository method(s)
    │
    ▼
Repository Layer
    │  executes SQLAlchemy ORM query
    │
    ▼
PostgreSQL Config DB
    │
    ◀─ (result rows)
    │
Service  →  Pydantic response schema
    │
Endpoint  →  JSONResponse / FastAPI auto-serialiser
    │
HTTP Client receives response
    │
[finally]  Session.close()  →  connection returned to QueuePool
```

### Read-Only Optimisation

GET endpoints use `Depends(get_db_readonly)` / `get_<name>_service_readonly`. The readonly session yields from a separate factory that **skips `COMMIT`** on success, saving ~0.1–0.5 ms per request by avoiding a round-trip flush to PostgreSQL.

---

## 2. Dependency Injection Map

```python
# app/api/deps.py
get_db()                      →  Session (read-write)
get_db_readonly()             →  Session (read-only, no COMMIT)

get_source_service()          →  SourceService(db)
get_destination_service()     →  DestinationService(db)
get_pipeline_service()        →  PipelineService(db)
get_backfill_service()        →  BackfillService(db)
get_tag_service()             →  TagService(db)
get_preset_service()          →  PresetService(db)
```

Each service is re-instantiated per request (FastAPI `Depends` default scope). The session is closed in the `finally` block of `get_db_session()` / `get_db_session_readonly()`.

---

## 3. Pipeline State Machine

```
                  POST /pipelines
                        │
                        ▼
                  status = PAUSE   ◀── always forced on creation
                        │
              PATCH /pipelines/{id}/start
                        │
                        ▼
                  status = START   ──▶  Compute picks up, spawns process
                        │
              PATCH /pipelines/{id}/pause
                        ▼
                  status = PAUSE   ──▶  Compute stops process (graceful)
                        │
              PATCH /pipelines/{id}/refresh
                        ▼
                  status = REFRESH  ──▶  Compute restarts with new config
                        │
                  (auto-transitions back to START after restart)
```

Status changes **must** update **both** `pipelines.status` and `pipeline_metadata.status` in the same transaction. Partial updates are treated as a bug.

---

## 4. Preview Flow (with Worker)

When `WORKER_ENABLED=true`:

```
POST /pipelines/{id}/preview
    │
    ▼
PipelineService.run_preview()
    │  check Redis cache: GET preview:{hash}
    │  (cache HIT → return immediately)
    │
    ▼ (cache MISS)
WorkerClient.submit_preview_task()
    │  celery_app.send_task("worker.preview.execute", ...)
    │
    ▼
Redis broker (db 1)  →  Celery Worker
    │  DuckDB executes query against source
    │  result serialized, stored in Redis results (db 2, 5 min TTL)
    │
    ▼
Backend polls task status via AsyncResult
    │  state: PENDING → PROGRESS → SUCCESS/FAILURE
    │
    ▼
Returns preview result JSON to client
```

When `WORKER_ENABLED=false`, `PipelinePreviewService` executes the DuckDB query synchronously in the Backend process.

---

## 5. Background Scheduler Flow

The scheduler runs in a **dedicated OS thread** (APScheduler `BackgroundScheduler`). Async monitor tasks are submitted to a **persistent event loop** running in a second daemon thread, preventing `asyncio.run()` from creating/destroying loops on every tick.

```
Main Thread (uvicorn)
    │
    ├─ APScheduler thread (9 jobs, interval-fire)
    │       │
    │       ├─ sync jobs: execute directly in APScheduler thread
    │       │     e.g. system_metric_collection, worker_health_check
    │       │
    │       └─ async jobs: run_coroutine_threadsafe(coro, shared_loop)
    │               │  blocks APScheduler thread until done (max 120 s)
    │               ▼
    │         Async Daemon Thread (persistent event loop)
    │               │  awaits monitor coroutines
    │               ▼
    │         PostgreSQL / Redis / external HTTP
    │
    └─ Request handler threads (from Uvicorn thread pool)
```

---

## 6. WebSocket Endpoint

`/ws/pipeline-status` provides a live push channel for pipeline status updates to the Web SPA.

```
Client connects WS
    │
    ▼  loop forever:
    ├─ query pipeline statuses from DB (every ~2 s)
    ├─ send JSON payload to client
    └─ await asyncio.sleep(2)
        │
Client disconnects → WebSocketDisconnect raised → connection closed cleanly
```

---

## 7. Error Response Schema

All errors return a consistent JSON envelope:

```json
{
  "error": {
    "code": "ENTITY_NOT_FOUND",
    "message": "Pipeline with id 42 not found",
    "details": {}
  }
}
```

| Exception Class          | HTTP Status | Code                    |
| ------------------------ | ----------- | ----------------------- |
| `EntityNotFoundError`    | 404         | `ENTITY_NOT_FOUND`      |
| `DuplicateEntityError`   | 409         | `DUPLICATE_ENTITY`      |
| `DatabaseError`          | 500         | `DATABASE_ERROR`        |
| `RosettaException`       | varies      | custom                  |
| `RequestValidationError` | 422         | `VALIDATION_ERROR`      |
| unhandled                | 500         | `INTERNAL_SERVER_ERROR` |

Responses for 500-class errors are sanitised through `error_sanitizer.sanitize_for_db()` before storage to prevent credential leakage in logs or DB error records.
