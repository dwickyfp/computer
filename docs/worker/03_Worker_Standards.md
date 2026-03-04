# WK-03 — Worker: Coding Standards & Style

## 1. Folder Structure

```
worker/
├── main.py              # Celery CLI entry point (imports celery_app)
├── server.py            # FastAPI health API — separate process in start.sh
├── start.sh             # Production start: server + celery worker (Linux/macOS)
├── start.ps1            # Production start: server + celery worker (Windows)
├── pyproject.toml       # Dependencies via uv
├── HOW_TO_RUN.md        # Quick-start guide
└── app/
    ├── celery_app.py    # Single Celery instance — import from here only
    ├── config/
    │   └── settings.py  # Pydantic BaseSettings, @lru_cache singleton
    ├── core/
    │   ├── logging.py   # structlog configuration
    │   ├── security.py  # AES-256-GCM encrypt/decrypt (mirrors backend)
    │   └── database.py  # psycopg2 helpers for reading config
    ├── services/
    │   └── source_service.py  # Fetch + decrypt source connection config
    └── tasks/
        ├── base.py            # BaseTask — shared on_failure/on_retry hooks
        ├── preview/
        │   ├── task.py        # @celery_app.task decorator
        │   ├── executor.py    # DuckDB execution orchestration
        │   ├── validator.py   # SQL safety validation
        │   ├── serializer.py  # DuckDB result → JSON dict
        │   └── profiler.py    # Column statistics
        ├── flow_task/
        ├── linked_task/
        ├── lineage/
        └── destination_table_list/
```

---

## 2. Naming Conventions

| Construct           | Convention                 | Example                                     |
| ------------------- | -------------------------- | ------------------------------------------- |
| Modules / packages  | `snake_case`               | `executor.py`, `source_service.py`          |
| Classes             | `PascalCase`               | `BaseTask`, `PreviewExecutor`               |
| Task names          | `worker.<domain>.<action>` | `worker.preview.execute`                    |
| Functions / methods | `snake_case`               | `execute_preview()`, `validate_sql()`       |
| Private members     | `_leading_underscore`      | `_get_connection()`                         |
| Constants           | `UPPER_SNAKE_CASE`         | `MAX_PREVIEW_ROWS = 1000`                   |
| Celery queue names  | lowercase                  | `"preview"`, `"default"`, `"orchestration"` |

---

## 3. Task Registration Pattern

```python
# All tasks follow this pattern:
@celery_app.task(
    base=BaseTask,           # Always use BaseTask for shared hooks
    name="worker.<domain>.<action>",   # Explicit name — never auto-generated
    bind=True,               # Access self (task instance, update_state)
    max_retries=1,           # Retry once on transient failure
    default_retry_delay=5,   # 5 seconds before retry
    queue="<queue_name>",    # Explicit queue routing
    acks_late=True,          # Ack only after completion
)
def my_task(self, arg1, arg2, ...):
    ...
```

**Rules:**

- Always specify `name=` explicitly — never rely on auto-generated names.
- Always use `base=BaseTask` for consistent error logging.
- Always specify `queue=` — never let tasks fall through to default routing accidentally.
- Stateless tasks: `reject_on_worker_lost=True` (global default — re-queue on crash).
- Stateful tasks (those that commit DB state mid-execution): explicitly set `reject_on_worker_lost=False`.

---

## 4. DuckDB Usage Pattern

```python
# Each task creates and closes its own connection:
import duckdb

def execute_preview(sql, source_id, ...):
    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"ATTACH '{dsn}' AS src (TYPE POSTGRES, READ_ONLY)")
        result = conn.execute(sql).fetchdf()
        return serialize_result(result)
    finally:
        conn.close()  # Always close — no connection pooling for DuckDB
```

- **Never share** a DuckDB connection between threads.
- Use `:memory:` database — DuckDB is a transformation engine, not a store.
- Always `CLOSE` the connection in a `finally` block.

---

## 5. Security Pattern for Credential Handling

```python
# Source credentials must always be decrypted immediately before use
# and never stored in a variable that outlives the function scope.

from app.core.security import decrypt_value

dsn = (
    f"host={source.host} "
    f"dbname={source.database} "
    f"user={source.username} "
    f"password={decrypt_value(source.encrypted_password)}"
)
# Use dsn immediately in ATTACH command
# Do not log dsn or store it in any persistent structure
```

---

## 6. Logging

Uses **structlog** for structured JSON logging:

```python
import structlog
logger = structlog.get_logger(__name__)

logger.info(
    "Preview task started",
    task_id=self.request.id,
    source_id=source_id,
    table_name=table_name,
)

logger.error(
    "Preview task failed",
    task_id=self.request.id,
    exc_info=True,
)
```

- Log at `INFO` for task start/finish.
- Log at `DEBUG` for intermediate steps (query text, row counts).
- Log at `ERROR` with `exc_info=True` for exceptions.
- Never log decrypted credentials or raw DSNs.

---

## 7. Configuration

`app/config/settings.py` uses `pydantic_settings.BaseSettings` with `@lru_cache`:

```python
@lru_cache
def get_settings() -> Settings:
    return Settings()
```

Key settings:

| Variable                    | Default | Description                          |
| --------------------------- | ------- | ------------------------------------ |
| `CELERY_BROKER_URL`         | —       | Redis broker (db 1)                  |
| `CELERY_RESULT_BACKEND`     | —       | Redis results (db 2)                 |
| `CREDENTIAL_ENCRYPTION_KEY` | —       | Must match Backend exactly           |
| `WORKER_CONCURRENCY`        | `4`     | Number of concurrent worker threads  |
| `TASK_SOFT_TIME_LIMIT`      | `300`   | Seconds before SoftTimeLimitExceeded |
| `TASK_HARD_TIME_LIMIT`      | `360`   | Seconds before SIGKILL               |
| `DATABASE_URL`              | —       | Config DB URL (read source configs)  |

---

## 8. Testing

```bash
# Run tests
uv run pytest tests/ -v

# With coverage
uv run pytest tests/ --cov=app
```

Test tasks synchronously using Celery's `task_always_eager=True`:

```python
@pytest.fixture(autouse=True)
def celery_eager(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    # Tasks execute synchronously in the test process
```

Avoid mocking DuckDB in unit tests — use a real DuckDB `:memory:` connection with synthesised test data.
