# WK-02 — Worker: Application Flow

## 1. Startup Sequence

```
start.sh
  │
  ├─ 1. python server.py &     ──▶  FastAPI health server on :8002
  │
  └─ 2. celery -A main worker
              --loglevel=info
              -Q preview,default,orchestration
              -c {WORKER_CONCURRENCY}
              --pool=threads
     │
     ├─ Celery connects to Redis broker (db 1)
     ├─ autodiscover_tasks([
     │       "app.tasks.preview",
     │       "app.tasks.lineage",
     │       "app.tasks.flow_task",
     │       "app.tasks.linked_task",
     │       "app.tasks.destination_table_list",
     │  ])
     ├─ worker_init signal fires → setup_logging()
     └─ Worker enters task consumption loop
```

---

## 2. Preview Task — Happy Path

```
[Backend]
  POST /pipelines/{id}/preview
      │
      ├─ Check Redis cache: GET preview:{hash}
      │     HIT:  return cached result  (end)
      │     MISS: continue
      │
      └─ WorkerClient.submit_preview_task(sql, source_id, destination_id, ...)
           │  celery_app.send_task("worker.preview.execute", args=[...])
           │  → Redis RPUSH (queue: preview)
           │
           └─ Return task_id to caller

[Worker Thread]
  BLPOP preview queue
      │
      ├─ execute_preview_task(sql, source_id, destination_id, table_name, ...)
      │
      ├─ task.update_state(state="PROGRESS", meta={...})  → visible to Backend
      │
      ├─ execute_preview(sql, source_id, destination_id, ...)
      │     │
      │     ├─ source_service.get_source(source_id) + decrypt credentials
      │     ├─ validator.validate_sql(sql)           — safety checks
      │     │
      │     ├─ DuckDB: INSTALL postgres; LOAD postgres;
      │     │          ATTACH 'host=... dbname=... user=... password=...' AS src (TYPE POSTGRES)
      │     │          SELECT ... FROM src.public.{table_name} LIMIT 1000
      │     │
      │     ├─ (optional) profiler.profile_columns(result) — stats per column
      │     │
      │     └─ serializer.serialize_result(duckdb_result)
      │           → {columns, column_types, data, profile?, error}
      │
      ├─ Store result in Redis: SET preview:{hash} result EX 300
      │
      └─ return result dict  → Celery stores in Redis results (db 2)

[Backend — polling]
  AsyncResult(task_id).get(timeout=30)
      │
      └─ state=SUCCESS → return result to HTTP client
```

---

## 3. Task State Machine

```
PENDING  ──(worker picks up task)──▶  STARTED
STARTED  ──(task.update_state)──────▶  PROGRESS
PROGRESS ──(task completes)─────────▶  SUCCESS
PROGRESS ──(exception raised)───────▶  FAILURE
FAILURE  ──(max_retries not exceeded)▶  RETRY  ──▶  PENDING again
FAILURE  ──(max_retries exceeded)────▶  FAILURE (permanent)
```

`task_track_started=True` ensures the `STARTED` state is visible to callers — preventing false PENDING reads on slow-starting tasks.

---

## 4. Error Handling Flow

```
execute_preview_task raises exception
    │
    ├─ BaseTask.on_failure() called
    │     logs structured error with task_id, args, exc info
    │
    ├─ max_retries=1 (one retry)
    │     self.retry(countdown=5)
    │     task transitions: FAILURE → RETRY → PENDING
    │
    └─ After max_retries exhausted:
          task state = FAILURE permanently
          Celery stores exception info in Redis results
          Backend receives failure on get(), returns error response to client
```

**`task_reject_on_worker_lost=True`** (global default): if the worker process crashes _mid-task_, the task is re-queued automatically. This is safe for preview tasks which are **stateless** — no partial state is written to the database.

**Exception for stateful tasks** (flow_task, linked_task): these tasks explicitly set `reject_on_worker_lost=False` because partial state may have already been committed; re-queuing would cause duplicate writes.

---

## 5. SQL Validation Flow

Before DuckDB executes any user-provided SQL:

```
validator.validate_sql(sql)
    │
    ├─ Check for dangerous statements:
    │     DROP, DELETE, TRUNCATE, UPDATE, INSERT, ALTER, CREATE, EXEC
    │     → raise ValidationError  (HTTP 422 back to caller)
    │
    ├─ Check for ; (multiple statements)
    │     → raise ValidationError
    │
    ├─ Check for -- or /* (comment injection)
    │     → raise ValidationError  (configurable)
    │
    └─ Allowed: SELECT, WITH (CTE), subqueries, LIMIT, ORDER BY
```

---

## 6. Data Serialisation Flow

Raw DuckDB result rows → JSON-serialisable structure:

```
serializer.serialize_result(duckdb_result)
    │
    ├─ columns: list[str]       — column names
    ├─ column_types: list[str]  — DuckDB inferred types
    └─ data: list[list[Any]]    — rows (paginated, max 1000 rows default)
         │
         Special type handling:
           datetime → ISO 8601 string
           Decimal  → float
           bytes    → base64 string
           None     → null
```

---

## 7. Optional Profiling Flow

When `include_profiling=True`:

```
profiler.profile_columns(duckdb_conn, table_name, columns)
    │
    For each column:
        ├─ COUNT(DISTINCT col)         → cardinality
        ├─ COUNT(*) FILTER (col IS NULL) → null_count
        ├─ MIN(col), MAX(col)          → range
        ├─ AVG(col), STDDEV(col)       → (numeric only)
        └─ value_counts (top 10 most frequent values)
    │
    Returns: dict[column_name → ProfileStats]
```

Profiling queries run against the **already-attached** DuckDB PostgreSQL scan — no extra round-trip to the source.

---

## 8. Concurrency Model

```
Celery Worker Process
    │
    ├─ Thread 1: task (DuckDB query)
    ├─ Thread 2: task (DuckDB query)
    ├─ Thread N: task (DuckDB query)  ← N = WORKER_CONCURRENCY env var
    └─ Celery heartbeat thread
```

- DuckDB instances are **not shared** between threads — each task creates and closes its own `duckdb.connect()`.
- Thread safety: DuckDB's Python API is thread-safe when each thread holds its own connection.
- Redis connections from `broker_pool_limit=10` are shared via thread-safe pool.
