# WK-04 — Worker: Business Rules & Constraints

## 1. Thread Pool Mandate

| Rule                              | Description                                                                                                                                                                                                 |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`--pool=threads` is mandatory** | DuckDB embeds a native library that cannot survive a `fork()`. Using `--pool=prefork` (Celery default) will cause silent corruption or crashes. The thread pool must always be used for this worker.        |
| **No multiprocessing in tasks**   | Tasks must not spawn child processes via `multiprocessing.Process`. Use `threading.Thread` within tasks if parallelism is needed, but prefer single-threaded task execution and rely on Celery concurrency. |

---

## 2. Credential Encryption Key

| Rule                               | Description                                                                                                                                                                                                                                    |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Key must match Backend exactly** | `CREDENTIAL_ENCRYPTION_KEY` must be byte-for-byte identical to the Backend's key. A mismatch causes `AESGCM.decrypt()` to raise `cryptography.exceptions.InvalidTag` — a **silent failure** that returns garbled plaintext, not a clear error. |
| **Key validation on startup**      | The worker validates key length at startup via `get_cipher()`. A wrong-length key causes immediate startup failure.                                                                                                                            |
| **Never log the key**              | The encryption key must never appear in logs, task args, or error messages.                                                                                                                                                                    |

---

## 3. Task Acknowledgement Rules

| Rule                      | Task Type                                | Setting                                         | Rationale                                                                 |
| ------------------------- | ---------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------- |
| Ack late, reject on crash | preview, lineage, destination_table_list | `acks_late=True`, `reject_on_worker_lost=True`  | Stateless — safe to re-run                                                |
| Ack late, do NOT re-queue | flow_task, linked_task                   | `acks_late=True`, `reject_on_worker_lost=False` | These tasks write partial state to DB; re-queuing causes duplicate writes |

---

## 4. SQL Safety Rules

| Rule                       | Description                                                                                                                        |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Whitelist-only SQL**     | Only `SELECT` and `WITH` (CTE) statements are permitted in preview SQL. Any DDL, DML, or DCL is rejected.                          |
| **No multiple statements** | Semicolons (`;`) inside the SQL body are forbidden — prevents stacked query injection.                                             |
| **Identifier injection**   | Table names and column names passed as parameters are quoted with DuckDB's identifier quoting before embedding in queries.         |
| **Max row limit**          | Preview queries are always wrapped with `LIMIT {MAX_PREVIEW_ROWS}` (default 1000). This cannot be overridden by user-provided SQL. |

---

## 5. DuckDB Session Rules

| Rule                           | Description                                                                                                                                              |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Per-task connection**        | Each task creates a fresh `duckdb.connect(":memory:")` and closes it in `finally`. Connections are never pooled or shared.                               |
| **Read-only attachment**       | Source PostgreSQL is attached with `READ_ONLY` flag to prevent accidental writes via DuckDB.                                                             |
| **Extension installation**     | `INSTALL postgres; LOAD postgres;` is run on every new DuckDB connection. This is idempotent and fast (extension is already downloaded after first run). |
| **No persistent DuckDB files** | DuckDB runs in `:memory:` mode exclusively. No `.duckdb` files are created on disk during task execution.                                                |

---

## 6. Result Cache Rules

| Rule                           | Description                                                                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Cache key is deterministic** | `sha256(sql + str(source_id) + str(destination_id) + table_name + filter_sql)` — same inputs always produce the same cache key.       |
| **TTL is 5 minutes**           | Cached results expire after 300 seconds. This balances freshness with redundant query prevention.                                     |
| **Cache is advisory**          | If Redis is unavailable, the task executes without caching — it does not fail. Cache misses are always safe.                          |
| **Cache is invalidated on**    | Cache TTL expiry only. Manual invalidation is not implemented — stale results within the 5-minute window are expected and acceptable. |

---

## 7. Time Limit Rules

| Limit             | Value | What Happens When Exceeded                                    |
| ----------------- | ----- | ------------------------------------------------------------- |
| `soft_time_limit` | 300 s | `SoftTimeLimitExceeded` raised inside task; task can clean up |
| `hard_time_limit` | 360 s | SIGKILL sent to task thread; task cannot clean up             |

Preview queries that are still running after 5 minutes are indicative of missing `LIMIT`, full-table scans on large tables, or network issues. Users should be advised to add a `LIMIT` to their preview SQL.

---

## 8. Retry Rules

| Rule                              | Description                                                                                                                                                                     |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **max_retries=1**                 | Each task retries once (giving 2 total attempts) on any exception.                                                                                                              |
| **Retry delay**                   | `default_retry_delay=5` seconds between attempts.                                                                                                                               |
| **No retry on validation errors** | `ValidationError` (bad SQL) should not be retried — fail immediately and report to the caller. Tasks should `raise exc` without calling `self.retry()` for validation failures. |

---

## 9. Health Reporting Rules

| Rule                                    | Description                                                                                                                                                                               |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Health endpoint must always respond** | The FastAPI health server runs in a separate OS process from the Celery worker. If the Celery worker hangs, the health server remains responsive.                                         |
| **Backend polls every 10 s**            | The Backend APScheduler job records the last-seen Worker health snapshot in `worker_health_status`. If the Worker goes offline, the Backend records a failure status — it does not crash. |
| **Worker optional**                     | `WORKER_ENABLED=false` is the safe default. The system operates fully without the Worker; preview tasks are executed synchronously in Backend.                                            |
