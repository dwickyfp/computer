# BE-04 — Backend: Business Rules & Constraints

## 1. Pipeline Lifecycle Rules

| Rule                      | Description                                                                                                                                                                            |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Force-PAUSE on create** | Every new pipeline is created with `status='PAUSE'`. Auto-start is not allowed. An explicit `PATCH /pipelines/{id}/start` call is required.                                            |
| **Dual-status update**    | Any status change must update **both** `pipelines.status` AND `pipeline_metadata.status` in the same transaction. Updating only one is a data integrity bug.                           |
| **Refresh guard**         | `PATCH /pipelines/{id}/refresh` only sets `ready_refresh=true` if `status='START'`. Paused pipelines are ignored (no-op).                                                              |
| **Cascade delete**        | Deleting a pipeline cascades to `pipeline_destinations`, `pipeline_metadata`, `pipelines_destination_table_sync`, and all related child records at the DB level (`ON DELETE CASCADE`). |
| **Name uniqueness**       | `pipelines.name` has a `UNIQUE` constraint. `DuplicateEntityError` (HTTP 409) is raised on collision.                                                                                  |
| **ROSETTA source**        | Pipelines with `source_type='ROSETTA'` have `source_id=NULL`. Endpoints must null-guard before passing `source_id` to `SourceService`. `chain_client_id` must be provided instead.     |

---

## 2. Source & Destination Rules

| Rule                      | Description                                                                                                                                                                                                   |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Credential encryption** | All passwords, DSNs, private keys stored in `sources` / `destinations` tables are encrypted with `encrypt_value()` before INSERT/UPDATE. They must be decrypted with `decrypt_value()` before use at runtime. |
| **Snowflake RSA key**     | Snowflake destinations use PKCS#8 encrypted private key files, not passwords. The decrypted key must be deserialised with `cryptography.hazmat.primitives.serialization` before use.                          |
| **Delete guard**          | A source or destination cannot be deleted while an active pipeline (`status='START'`) references it. The service layer enforces this check before calling the repository.                                     |

---

## 3. Backfill Rules

| Rule                    | Description                                                                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Queue-based**         | Backfill jobs are enqueued in `queue_backfill_data` with `status='PENDING'`. Compute polls this table every 5 s.                                   |
| **Checkpoint resume**   | `last_pk_value` tracks the last processed primary key. On Compute restart, jobs resume from checkpoint rather than re-starting from the beginning. |
| **Max resume attempts** | After `MAX_RESUME_ATTEMPTS=3` failed resumes, the job is marked `FAILED`.                                                                          |
| **Status transitions**  | `PENDING → EXECUTING → COMPLETED / FAILED`. Transitions are atomic via DB-level row locks.                                                         |

---

## 4. WAL Monitoring Rules

| Rule                      | Description                                                                                                                                          |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Threshold alerts**      | WAL size thresholds are configurable via `rosetta_setting_configuration` table (keys `wal_warning_threshold_gb`, `wal_critical_threshold_gb`).       |
| **Notification triggers** | When WAL size crosses a threshold, a notification record is inserted and the notification_sender job dispatches webhook/Telegram alerts within 30 s. |
| **Minimum interval**      | `wal_monitor_interval_seconds` must be ≥ 60 (validated in Settings). Prevents overwhelming the source DB with replication slot queries.              |

---

## 5. Schema Evolution Rules

| Rule                          | Description                                                                                                                                 |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| **Non-destructive first**     | Schema changes (ADD COLUMN, RENAME) are flagged but do not automatically stop a pipeline. An alert is raised and human review is required.  |
| **Breaking change detection** | DROP COLUMN, TYPE CHANGE operations trigger `SCHEMA_BREAKING_CHANGE` status on the affected `pipeline_metadata` row, pausing that pipeline. |
| **History retention**         | All detected changes are logged to `history_schema_evolution` for audit.                                                                    |

---

## 6. Rosetta Chain Rules

| Rule                     | Description                                                                                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Key one-time reveal**  | `GET /chain/key` returns a masked key. `GET /chain/key/reveal` returns the full raw key. The reveal endpoint does not re-encrypt after read — the chain key stays encrypted at rest. |
| **Toggle requires body** | `PATCH /chain/toggle-active` requires a JSON body `{"is_active": bool}` via `ChainToggleActiveRequest`. Query parameters are not accepted.                                           |
| **Client response**      | `ChainClientResponse` does **not** include `chain_key` or `description`. These fields must never be added to the response schema.                                                    |
| **Single config row**    | There is exactly one row in `rosetta_chain_config`. Upsert behaviour (not create) is used on key regeneration.                                                                       |

---

## 7. Security Rules

| Rule                   | Description                                                                                                                                                                       |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **AES-256-GCM**        | All credential encryption uses AES-256-GCM. Padding schemes or CBC mode are not used.                                                                                             |
| **Key length**         | `CREDENTIAL_ENCRYPTION_KEY` must be exactly 32 bytes (or base64-encoded 32 bytes). The application **fails loudly** on startup if length is wrong — no silent truncation/padding. |
| **Nonce uniqueness**   | Each `encrypt_value()` call generates a fresh 12-byte random nonce via `os.urandom(12)`. The same plaintext will produce different ciphertext on every call.                      |
| **Error sanitisation** | All exception messages are passed through `sanitize_for_db()` / `sanitize_for_log()` before being written to the database or logs to strip any embedded credentials.              |
| **CORS**               | Allowed origins are configured via `CORS_ORIGINS` env var (default `["*"]`). Restrict this in production.                                                                         |
| **API Key**            | Optional `API_KEY` env var. When set, all requests must include `X-API-Key` header.                                                                                               |

---

## 8. Data Integrity Rules

| Rule                         | Description                                                                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Timestamps in UTC+7**      | `updated_at` is always written in Asia/Jakarta (UTC+7) by `BaseRepository.update()`. `created_at` is set on INSERT.                 |
| **`expire_on_commit=False`** | ORM objects remain accessible after session commit. This prevents `DetachedInstanceError` in response serialisation.                |
| **`QueuePool` pre-ping**     | `db_pool_pre_ping=True` — stale connections are tested before use, preventing "server closed connection" errors after idle periods. |
| **Pool LIFO**                | `db_pool_use_lifo=True` — most recently used connections are reused first, improving TCP keep-alive hit rate.                       |
