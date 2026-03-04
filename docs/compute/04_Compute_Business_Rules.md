# CO-04 — Compute: Business Rules & Constraints

## 1. Pipeline Process Rules

| Rule                         | Description                                                                                                                                                                                           |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **One process per pipeline** | `PipelineManager` enforces a 1:1 mapping between a running pipeline and an OS process. If a process for `pipeline_id=X` is already alive, a duplicate is never spawned.                               |
| **Stale config detection**   | If `pipeline.updated_at` changes between polls while a process is alive, the manager tears down the old process and spawns a new one to pick up the updated config.                                   |
| **Graceful stop timeout**    | When stopping a process, `stop_event.set()` is called first. If the process has not exited within the configured timeout, `process.terminate()` is called. If still alive, `process.kill()`.          |
| **Zombie cleanup**           | After a process exits ungracefully (crash), the manager detects `not process.is_alive()` on the next poll and respawns.                                                                               |
| **Missing pipeline guard**   | If `PipelineRepository.get_by_id(pipeline_id)` returns `None` inside the child process (pipeline deleted between schedule and start), the process logs a warning and exits cleanly — no error raised. |

---

## 2. CDC Operation Mapping

| Debezium op | `CDCRecord.operation` | Action at Destination         |
| ----------- | --------------------- | ----------------------------- |
| `c`         | create                | INSERT / MERGE (upsert)       |
| `u`         | update                | MERGE (upsert)                |
| `d`         | delete                | DELETE WHERE pk               |
| `r`         | read (snapshot)       | INSERT / MERGE (initial load) |

- `before` is `None` for `c` and `r` operations.
- `after` is `None` for `d` operations.
- Implementations must not assume non-nullable `before`/`after`.

---

## 3. Backfill Rules

| Rule                                | Description                                                                                                                                                                            |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Row-level checkpoint**            | `last_pk_value` is updated after every successful batch write. On resume, query starts at `pk > last_pk_value`.                                                                        |
| **Batch size**                      | Default 10,000 rows per DuckDB batch. Configurable via `BackfillManager(batch_size=N)`.                                                                                                |
| **STALE_JOB_THRESHOLD_MINUTES = 0** | All jobs in `EXECUTING` state are recovered immediately on startup (threshold of 0 means no grace period — treat all stale jobs as recoverable).                                       |
| **MAX_RESUME_ATTEMPTS = 3**         | After 3 chekpoint resets, the job is permanently failed. A human must diagnose the failure and reset the job manually.                                                                 |
| **Identifier safety**               | All table/column names passed into DuckDB SQL via `_validate_identifier()` before embedding in query strings.                                                                          |
| **DuckDB postgres extension**       | Backfill uses DuckDB's `postgres` extension to read from source and write to target. DuckDB itself is not a permanent store — it is used purely as an in-memory transformation engine. |

---

## 4. DLQ Rules

| Rule                        | Description                                                                                                                                                |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Per-destination streams** | Each `(source_id, table_name, destination_id)` combination has its own Redis Stream. Failures in one destination do not pollute another.                   |
| **Stream key format**       | `dlq:{source_id}:{table_name}:{destination_id}`                                                                                                            |
| **Max stream length**       | Configurable; enforced with Redis `MAXLEN ~` (approximate trimming for performance).                                                                       |
| **Retry back-off**          | DLQRecoveryWorker applies exponential back-off between retry attempts.                                                                                     |
| **Permanent failure**       | After `max_retries` exhausted, the DLQ record is moved to a dead-records log and the corresponding `pipeline_metadata` row is updated to `status='ERROR'`. |

---

## 5. Rosetta Chain Rules

| Rule                           | Description                                                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Chain key validation**       | Every ingest request must carry `X-Chain-Key` header. The server validates it against the AES-256-GCM decrypted key stored in `rosetta_chain_config`. Requests with missing or invalid keys receive HTTP 401. |
| **Stream retention**           | Chain Redis Streams are trimmed by `MINID` (minimum entry ID based on timestamp) with a configurable retention window (`CHAIN_STREAM_RETENTION_DAYS`). Untrimmed streams will grow unboundedly.               |
| **Arrow IPC format**           | Senders must serialise data as Apache Arrow IPC stream format (`pyarrow.ipc.new_stream`). Other formats are rejected.                                                                                         |
| **XREADGROUP consumer groups** | Each pipeline that consumes from a chain creates a dedicated consumer group. Acknowledgement (`XACK`) is only sent after a successful write to the downstream destination.                                    |
| **Schema sync required**       | Before streaming data, senders must `POST /chain/schema` to register the table schema. Without a registered schema, the `ChainPipelineEngine` cannot deserialise entries.                                     |

---

## 6. Snowflake Destination Rules

| Rule                            | Description                                                                                                                                                                  |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **No Snowflake Connector SDK**  | The implementation uses the **Snowpipe Streaming REST API** directly with JWT authentication. The `snowflake-connector-python` package is not used.                          |
| **RSA key-pair authentication** | Snowflake destinations require a PKCS#8 private key file (PEM format). Passphrase is stored encrypted. The key is decrypted and deserialised with `cryptography` at runtime. |
| **JWT expiry**                  | JWT tokens are short-lived (configurable, typically 1 h). The destination adapter regenerates tokens before expiry.                                                          |
| **Batch ingestion**             | Records are batched and submitted via `insertRows` API. The `insertDedup` flag is set to enable server-side deduplication on Snowflake's side.                               |

---

## 7. Schema Validation Rules

| Rule                         | Description                                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Type compatibility check** | Before writing a batch, `schema_validator.py` verifies that source column types are compatible with the destination column types. |
| **Non-blocking**             | Incompatible types log a warning but do not stop the pipeline. The individual record is sent to DLQ.                              |
| **New columns**              | Columns appearing in CDC events but not in the destination schema are ignored by default (configurable).                          |
| **Column case sensitivity**  | Column name matching is case-insensitive for PostgreSQL destinations (PostgreSQL folds unquoted identifiers to lowercase).        |

---

## 8. Security Rules

| Rule                                 | Description                                                                                                                                                                               |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Credential decryption at runtime** | Connection credentials are decrypted once at engine start. Plaintext credentials are never written to disk or logs.                                                                       |
| **Error sanitisation**               | All exception strings are passed through `sanitize_for_log()` / `sanitize_for_db()` before being written - prevents credentials from appearing in error metadata stored in the Config DB. |
| **JVM isolation**                    | Each pipeline process runs its own JVM. A JVM crash in one pipeline process does not affect other pipelines.                                                                              |
