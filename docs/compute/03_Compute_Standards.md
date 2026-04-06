# CO-03 — Compute: Coding Standards & Style

## 1. Folder Structure

```
compute/
├── main.py              # Entry point: init pool, start PipelineManager
├── server.py            # Chain HTTP server (FastAPI, background thread)
├── config/
│   └── config.py        # Pydantic BaseSettings — get_config() singleton
├── core/
│   ├── manager.py       # PipelineManager + PipelineProcess dataclass
│   ├── engine.py        # PipelineEngine (Debezium wrapper)
│   ├── chain_engine.py  # ChainPipelineEngine (Redis Stream consumer)
│   ├── event_handler.py # CDCEventHandler — event parsing + routing
│   ├── backfill_manager.py  # BackfillManager + DuckDB batch jobs
│   ├── dlq_manager.py   # DLQManager — Redis Stream DLQ writes
│   ├── dlq_recovery.py  # DLQRecoveryWorker — retry thread
│   ├── repository.py    # Direct psycopg2 queries (no ORM)
│   ├── models.py        # Python dataclasses for DB rows
│   ├── schema_validator.py  # Column compatibility checks
│   ├── security.py      # AES-256-GCM (mirrors backend/core/security.py)
│   ├── error_sanitizer.py   # Strips credentials from error messages
│   ├── notification.py  # Notification log inserts
│   ├── database.py      # psycopg2 ThreadedConnectionPool wrapper
│   ├── exceptions.py    # PipelineException, DestinationException
│   └── timezone.py      # UTC+7 helpers
├── sources/
│   ├── base.py          # BaseSource ABC
│   └── postgresql.py    # PostgreSQLSource — builds Debezium connector config
├── destinations/
│   ├── base.py          # BaseDestination ABC, CDCRecord dataclass
│   ├── postgresql.py    # PostgreSQLDestination (DuckDB MERGE INTO)
│   ├── rosetta.py       # RosettaDestination (Arrow IPC HTTP)
│   └── snowflake/       # SnowflakeDestination (Snowpipe Streaming REST)
├── chain/
│   ├── auth.py          # X-Chain-Key validation
│   ├── ingest.py        # ChainIngestManager (Arrow IPC → Redis Stream)
│   └── schema.py        # ChainSchemaManager (table schema registry)
├── requirements.txt
└── pyproject.toml
```

---

## 2. Naming Conventions

| Construct           | Convention                                               | Example                             |
| ------------------- | -------------------------------------------------------- | ----------------------------------- |
| Modules / packages  | `snake_case`                                             | `event_handler.py`                  |
| Classes             | `PascalCase`                                             | `PipelineEngine`, `CDCEventHandler` |
| Dataclasses         | `PascalCase`                                             | `CDCRecord`, `PipelineProcess`      |
| Functions / methods | `snake_case`                                             | `_build_routing_table()`            |
| Private / internal  | `_leading_underscore`                                    | `_routing_table`, `_redis`          |
| Constants           | `UPPER_SNAKE_CASE`                                       | `MAX_RESUME_ATTEMPTS = 3`           |
| Thread names        | Descriptive strings                                      | `"chain_cleanup"`, `"backfill"`     |
| Redis Stream keys   | `dlq:{src}:{table}:{dst}` / `rosetta:chain:{id}:{table}` |

---

## 3. Process & Thread Model

| Concern            | Primitive                                   | Rationale                                                   |
| ------------------ | ------------------------------------------- | ----------------------------------------------------------- |
| Pipeline isolation | `multiprocessing.Process`                   | JVM cannot be shared or forked; each pipeline needs own JVM |
| Backfill           | `threading.Thread`                          | CPU-light DB I/O; shares parent process pool                |
| DLQ recovery       | `threading.Thread`                          | Runs inside each pipeline child process                     |
| Chain cleanup      | `threading.Thread`                          | Periodic Redis trimming; low frequency                      |
| Stop coordination  | `multiprocessing.Event` / `threading.Event` | Graceful shutdown signalling                                |

**Rule:** Never use `multiprocessing.fork` after JVM startup — always use `spawn` start method on platforms that support it. The JVM must be started _inside_ the spawned process.

---

## 4. Database Access Pattern

The Compute service uses **raw psycopg2** — no SQLAlchemy ORM. This is intentional: the ORM overhead is unnecessary in a pipeline process that runs a tight event loop.

```python
# Correct pattern:
conn = get_db_connection()
try:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT ... FROM pipelines WHERE id = %s", (pipeline_id,))
        row = cur.fetchone()
    conn.commit()
finally:
    return_db_connection(conn)
```

- All queries use `%s` placeholders — never f-strings for SQL.
- Use `RealDictCursor` to get `dict`-like rows.
- `return_db_connection()` must be called in `finally` to prevent pool exhaustion.

---

## 5. SQL Injection Prevention

SQL identifier validation must be used for any table or column name embedded in dynamic SQL:

```python
# In backfill_manager.py:
_validate_identifier(table_name)   # raises ValueError on unsafe input

# Regex: ^[A-Za-z_][A-Za-z0-9_.]*$
# Allows schema.table notation; rejects any injection vector
```

Only use `%s` for value parameters. Never use f-strings or `.format()` to insert values.

---

## 6. CDCRecord Contract

All destination adapters operate on `CDCRecord` objects:

```python
@dataclass
class CDCRecord:
    operation: str      # 'c' = create, 'u' = update, 'd' = delete, 'r' = read/snapshot
    table_name: str     # source table name (schema.table)
    before: dict | None # row state before change (None for 'c', 'r')
    after: dict | None  # row state after change (None for 'd')
    source_ts_ms: int   # Debezium event timestamp (epoch ms)
    transaction_id: str | None
```

Destinations must handle all four operation types. Unrecognised operations should be logged and skipped (not raise).

---

## 7. Error Handling Strategy

```
Destination.write() failure
    │
    ├─ Transient error (network, timeout):
    │     retry N times with back-off
    │     still failing → push to DLQ
    │
    ├─ Permanent error (schema mismatch, auth failure):
    │     push to DLQ with error metadata
    │     update pipeline_metadata.status = 'ERROR'
    │     log sanitised error (sanitize_for_log)
    │
    └─ Process-level unrecoverable error:
          engine exits → PipelineManager detects dead process
          PipelineManager respawns process on next poll
```

---

## 8. Logging

Compute uses Python's stdlib `logging`:

```python
logger = logging.getLogger(__name__)
# OR in a child process:
logger = logging.getLogger(f"Pipeline_{pipeline_id}")
```

- Child processes **reinitialise logging** with `logging.basicConfig(..., force=True)` because parent handlers are not inherited across `multiprocessing.Process`.
- `jpype`, `urllib3`, `httpx` loggers are set to `WARNING` to reduce noise.
- Never log raw passwords or keys — use `sanitize_for_log()` first.

---

## 9. Configuration

Compute uses `config/config.py` with Pydantic `BaseSettings`:

```python
config = get_config()  # @lru_cache singleton
config.chain.enabled          # bool
config.chain.redis_stream_prefix  # str
config.dlq.redis_url          # str
```

All environment variables are documented in `.env.example`.
