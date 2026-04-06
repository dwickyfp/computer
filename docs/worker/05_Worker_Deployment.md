# WK-05 — Worker: Deployment & Setup

## 1. Prerequisites

| Dependency | Minimum Version | Notes                                         |
| ---------- | --------------- | --------------------------------------------- |
| Python     | 3.11+           | Type hints, match statements                  |
| Redis      | 6+              | Celery broker (db 1) + result backend (db 2)  |
| PostgreSQL | 14+             | Config DB access (to read source credentials) |
| uv         | 0.4+            | Package manager                               |
| DuckDB     | 0.9+            | Bundled via `duckdb` pip package              |

---

## 2. Local Development Setup

```bash
# 1. Enter worker directory
cd worker

# 2. Install dependencies
uv sync

# 3. Start Redis + Config DB
docker-compose -f ../docker-compose.yml up -d

# 4. Copy and configure .env
cp .env.example .env
# Edit .env

# 5a. Start with the provided script (recommended)
./start.sh        # Linux/macOS
./start.ps1       # Windows PowerShell

# 5b. Or start manually (two terminals):
#   Terminal 1: FastAPI health server
uv run python server.py

#   Terminal 2: Celery worker
uv run celery -A main worker \
  --loglevel=info \
  -Q preview,default,orchestration \
  -c 4 \
  --pool=threads
```

---

## 3. Required Environment Variables

| Variable                    | Required | Default                    | Description                              |
| --------------------------- | -------- | -------------------------- | ---------------------------------------- |
| `CELERY_BROKER_URL`         | ✅       | —                          | `redis://localhost:6379/1`               |
| `CELERY_RESULT_BACKEND`     | ✅       | —                          | `redis://localhost:6379/2`               |
| `CREDENTIAL_ENCRYPTION_KEY` | ✅       | —                          | Must be identical to Backend's key       |
| `DATABASE_URL`              | ✅       | —                          | Config DB URL (read source configs)      |
| `WORKER_CONCURRENCY`        | ➖       | `4`                        | Number of concurrent task threads        |
| `TASK_SOFT_TIME_LIMIT`      | ➖       | `300`                      | Seconds before soft timeout              |
| `TASK_HARD_TIME_LIMIT`      | ➖       | `360`                      | Seconds before hard kill                 |
| `LOG_LEVEL`                 | ➖       | `INFO`                     | Logging verbosity                        |
| `REDIS_URL`                 | ➖       | `redis://localhost:6379/0` | Redis for caching preview results (db 0) |

---

## 4. Celery Worker CLI Reference

```bash
# Standard production start
celery -A main worker \
  --loglevel=info \
  -Q preview,default,orchestration \
  -c 4 \
  --pool=threads

# Debug mode (single thread, verbose)
celery -A main worker \
  --loglevel=debug \
  -Q preview,default \
  -c 1 \
  --pool=threads

# Inspect active tasks
celery -A main inspect active

# Inspect registered tasks
celery -A main inspect registered

# Purge a queue
celery -A main purge -Q preview

# Monitor with flower (install separately)
celery -A main flower --port=5555
```

---

## 5. Docker Deployment

The Worker has its own `Dockerfile`:

```bash
# Build
docker build -f worker/Dockerfile -t rosetta-worker .

# Run
docker run -d \
  --name rosetta-worker \
  -p 8002:8002 \
  -e CELERY_BROKER_URL="redis://redis:6379/1" \
  -e CELERY_RESULT_BACKEND="redis://redis:6379/2" \
  -e CREDENTIAL_ENCRYPTION_KEY="..." \
  -e DATABASE_URL="postgresql://..." \
  rosetta-worker
```

In the full-stack Docker Compose, Worker runs as a separate container. See `docker-compose-app.yml`.

---

## 6. Scaling

The Worker is horizontally scalable. Multiple Worker containers can consume from the same Redis queues simultaneously:

```bash
# Scale to 3 replicas (Docker Compose)
docker-compose -f docker-compose-app.yml up --scale worker=3 -d
```

For high-throughput preview workloads:

- Increase `WORKER_CONCURRENCY` per instance (recommend 4–8 threads).
- Add more Worker replicas.
- Ensure Redis `broker_pool_limit` per worker is not exhausted (default 10 per worker).

---

## 7. Health Check

```bash
# Manual health check
curl http://localhost:8002/health

# Expected response:
{"status": "healthy", "worker_concurrency": 4, "active_tasks": 0}

# If Celery worker is not running but server.py is:
{"status": "degraded", "error": "Cannot reach Celery worker"}
```

---

## 8. Monitoring with Flower

```bash
pip install flower
celery -A main flower --port=5555 --broker=redis://localhost:6379/1
# Open http://localhost:5555
```

Flower provides a web UI showing:

- Active, scheduled, and reserved tasks
- Worker connection status
- Task success/failure rates
- Queue lengths

---

## 9. Production Checklist

- [ ] `--pool=threads` flag is set — **never** use `prefork` or `gevent`
- [ ] `CREDENTIAL_ENCRYPTION_KEY` matches Backend exactly
- [ ] Redis persistence (`appendonly yes`) to survive restarts
- [ ] `TASK_SOFT_TIME_LIMIT` and `TASK_HARD_TIME_LIMIT` tuned for source query latency
- [ ] `WORKER_CONCURRENCY` set to ≤ number of CPU threads (DuckDB is CPU-bound)
- [ ] Celery visibility timeout (`visibility_timeout=3600`) ≥ `TASK_HARD_TIME_LIMIT`
- [ ] Worker health endpoint reachable from Backend (`WORKER_HEALTH_URL` configured)
- [ ] Flower or alternative monitoring deployed for task visibility
- [ ] Log rotation configured (stdout → Docker/systemd journal)
