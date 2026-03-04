# CO-05 — Compute: Deployment & Setup

## 1. Prerequisites

| Dependency          | Minimum Version | Notes                                                          |
| ------------------- | --------------- | -------------------------------------------------------------- |
| Python              | 3.11+           | Process model and type hints                                   |
| Java (JRE/JDK)      | 11+             | Required by Debezium via JPype / pydbzengine                   |
| PostgreSQL (source) | 10+             | Must have `wal_level=logical` and `max_replication_slots >= 1` |
| Redis               | 6+              | DLQ streams, Chain streams                                     |
| DuckDB              | 0.9+            | Backfill + PostgreSQL destination writes                       |
| pydbzengine         | pinned          | Ships Debezium JARs; see requirements.txt                      |

---

## 2. Local Development Setup

```bash
# 1. Enter compute directory
cd compute

# 2. Create virtual environment and install dependencies
pip install -r requirements.txt
# OR with uv:
uv sync

# 3. Start infrastructure
docker-compose -f ../docker-compose.yml up -d

# 4. Copy and configure .env
cp .env.example .env
# Edit .env — especially CONFIG_DATABASE_URL and any source credentials

# 5. Run compute engine
CONFIG_DATABASE_URL=postgresql://rosetta:rosetta@localhost:5433/rosetta python main.py
```

The Compute engine will:

1. Run `migrations/001_create_table.sql` (idempotent)
2. Start the PipelineManager polling loop
3. Start the BackfillManager thread
4. (If `CHAIN_ENABLED=true`) Start the Chain HTTP server thread

---

## 3. Required Environment Variables

| Variable                      | Required | Default                    | Description                                    |
| ----------------------------- | -------- | -------------------------- | ---------------------------------------------- |
| `CONFIG_DATABASE_URL`         | ✅       | —                          | PostgreSQL DSN for Config DB                   |
| `CREDENTIAL_ENCRYPTION_KEY`   | ✅       | —                          | Must match Backend's key exactly (AES-256-GCM) |
| `REDIS_URL`                   | ✅ (DLQ) | `redis://localhost:6379/0` | Redis for DLQ and Chain streams                |
| `CHAIN_ENABLED`               | ➖       | `false`                    | `true` to enable Chain HTTP ingest server      |
| `SERVER_HOST`                 | ➖       | `0.0.0.0`                  | Host for Chain HTTP server                     |
| `SERVER_PORT`                 | ➖       | `8001`                     | Port for Chain HTTP server + health API        |
| `PIPELINE_POOL_MAX_CONN`      | ➖       | `20`                       | Max psycopg2 connections per pipeline process  |
| `JVM_MAX_HEAP`                | ➖       | `16G`                      | JVM max heap size (e.g., `8G`, `16G`)          |
| `LOG_LEVEL`                   | ➖       | `INFO`                     | `DEBUG` / `INFO` / `WARNING` / `ERROR`         |
| `CHAIN_STREAM_RETENTION_DAYS` | ➖       | `7`                        | How many days to retain Chain Redis Streams    |
| `CHAIN_TRIM_INTERVAL_SECONDS` | ➖       | `3600`                     | How often to run the stream cleanup thread     |

---

## 4. PostgreSQL Source Requirements

The source PostgreSQL database must be configured before creating a pipeline:

```sql
-- In postgresql.conf (or via ALTER SYSTEM):
wal_level = logical             -- required for Debezium
max_replication_slots = 10      -- allow multiple pipelines
max_wal_senders = 10

-- Replication permission for the user:
ALTER ROLE rosetta_user REPLICATION;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rosetta_user;
```

Debezium creates a **replication slot** per pipeline. The slot name is deterministic based on pipeline ID and must be dropped manually if a pipeline is permanently deleted.

---

## 5. Running with Specific Pipeline (Debug Mode)

```bash
# Run only pipeline ID 5 (skips PipelineManager polling loop)
PIPELINE_ID=5 python main.py

# Enable debug logging
DEBUG=true LOG_LEVEL=DEBUG python main.py
```

---

## 6. Docker Deployment

```bash
# Build standalone Compute image
docker build -f Dockerfile.compute -t rosetta-compute .

# Run with env vars
docker run -d \
  --name rosetta-compute \
  -p 8001:8001 \
  -e CONFIG_DATABASE_URL="postgresql://..." \
  -e CREDENTIAL_ENCRYPTION_KEY="..." \
  -e REDIS_URL="redis://redis:6379/0" \
  rosetta-compute
```

In the production Docker Compose setup, Compute runs as a supervised process alongside Backend and Worker inside a single container managed by `supervisord`.

---

## 7. Health Endpoint

```
GET http://localhost:8001/health

Response:
{
  "status": "healthy",
  "pipelines_running": 3,
  "backfill_active_jobs": 1
}
```

---

## 8. Replication Slot Cleanup

Debezium creates a PostgreSQL replication slot per pipeline. If a pipeline is deleted without proper shutdown, the slot may remain on the source DB and prevent WAL cleanup (causing disk pressure).

```sql
-- List active replication slots:
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots;

-- Drop a stale slot:
SELECT pg_drop_replication_slot('rosetta_pipeline_5');
```

A WAL monitoring script in `scripts/` helps detect stale slots.

---

## 9. Production Checklist

- [ ] Source DB has `wal_level=logical` and adequate `max_replication_slots`
- [ ] `JVM_MAX_HEAP` tuned for expected number of concurrent pipelines (16G default is for large deployments)
- [ ] `PIPELINE_POOL_MAX_CONN` set based on pipeline count × 20 ≤ PostgreSQL `max_connections`
- [ ] Redis persistence enabled (`appendonly yes`) to survive restarts without losing DLQ/Chain data
- [ ] Replication slot monitoring alert configured (WAL monitor threshold in `rosetta_setting_configuration`)
- [ ] `CHAIN_ENABLED=true` only if inter-instance streaming is needed (adds HTTP server overhead otherwise)
- [ ] Log rotation configured for stdout (managed by Docker/systemd journal or supervisord)
