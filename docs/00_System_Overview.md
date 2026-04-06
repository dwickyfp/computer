# 00 — Rosetta ETL Platform: System Overview

## 1. What Is Rosetta?

Rosetta is a **production-grade, real-time ETL (Extract, Transform, Load) platform** that replicates data from PostgreSQL sources to multiple destination systems — including Snowflake, other PostgreSQL databases, and peer Rosetta instances — using Change Data Capture (CDC) via Debezium.

It provides a web-based admin dashboard for configuration management, live pipeline monitoring, SQL preview/profiling, query scheduling, smart tagging, backfill management, and inter-instance data chaining — all without requiring a third-party message broker for the core replication path.

---

## 2. Four-Service Architecture

Rosetta is composed of four independently deployable services that share a single PostgreSQL configuration database.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Shared PostgreSQL Config DB  (port 5433)                  │
│         pipelines · sources · destinations · metadata · queues               │
└───────┬────────────────┬───────────────┬────────────────┬────────────────────┘
        │                │               │                │
   ┌────▼─────┐   ┌──────▼──────┐  ┌────▼──────┐  ┌─────▼──────┐
   │  Backend │   │   Compute   │  │   Worker  │  │    Web     │
   │  :8000   │   │   :8001     │  │   :8002   │  │   :5173    │
   │ FastAPI  │   │  Debezium   │  │  Celery   │  │  React 19  │
   │ (Python) │   │  (Python)   │  │ (Python)  │  │ TypeScript │
   └──────────┘   └─────────────┘  └───────────┘  └────────────┘
```

| Service     | Role                                                                |
| ----------- | ------------------------------------------------------------------- |
| **Backend** | REST API for CRUD on all config entities. Hosts APScheduler jobs.   |
| **Compute** | CDC engine. Spawns per-pipeline OS processes. Routes data to sinks. |
| **Worker**  | Celery worker. Executes heavyweight preview queries in threads.     |
| **Web**     | React SPA admin dashboard. TanStack Router + Query + Table.         |

---

## 3. Shared Infrastructure

| Resource    | Port | Purpose                                                                               |
| ----------- | ---- | ------------------------------------------------------------------------------------- |
| Config DB   | 5433 | Single source of truth for all service configuration (PostgreSQL)                     |
| Source DB   | 5434 | CDC source (PostgreSQL with `wal_level=logical`)                                      |
| Target DB 1 | 5435 | PostgreSQL replication destination 1                                                  |
| Target DB 2 | 5436 | PostgreSQL replication destination 2                                                  |
| Redis       | 6379 | Cache (db 0), Celery broker (db 1), Celery results (db 2), DLQ Streams, Chain Streams |

---

## 4. Data Replication Destinations

| Destination Type | Mechanism                                            |
| ---------------- | ---------------------------------------------------- |
| **Snowflake**    | Snowpipe Streaming REST API + JWT RSA key-pair auth  |
| **PostgreSQL**   | DuckDB `MERGE INTO` via PostgreSQL scanner           |
| **Rosetta**      | Arrow IPC HTTP POST to peer Rosetta Compute instance |

---

## 5. Cross-Service Communication

```
Web ──HTTP──▶ Backend API ──SQL──▶ Config DB
                                       ▲
Compute ───────────────────────────────┘  (polls every 10 s)
Worker  ───────────────────────────────┘  (reads credentials)

Backend  ──Redis RPUSH──▶ Worker (Celery tasks when worker_enabled=true)
Worker   ──Redis SET──▶   Redis Results (cached 5 min TTL)

Compute (sender)   ──Arrow IPC HTTP──▶ Compute (receiver) /chain/ingest
Compute (receiver) ──Redis XADD──▶    rosetta:chain:{id}:{table}
Compute (ChainEngine) ──Redis XREADGROUP──▶ Destination
```

---

## 6. Security Model

All sensitive data — source passwords, destination credentials, Snowflake private keys, chain keys — are encrypted at rest using **AES-256-GCM** with a 96-bit random nonce per value.

- **Stored format:** `base64(nonce[12] || ciphertext || tag)`
- **Key source:** `CREDENTIAL_ENCRYPTION_KEY` environment variable (must be identical on Backend and Worker)
- **Chain key:** encrypted in `rosetta_chain_config`; only fully decryptable via `GET /chain/key/reveal` (one-time reveal endpoint)

---

## 7. Reading Order

| #     | Document                                      |
| ----- | --------------------------------------------- |
| 00    | `docs/00_System_Overview.md` ← You are here   |
| BE-01 | `docs/backend/01_Backend_Architecture.md`     |
| BE-02 | `docs/backend/02_Backend_Flow.md`             |
| BE-03 | `docs/backend/03_Backend_Standards.md`        |
| BE-04 | `docs/backend/04_Backend_Business_Rules.md`   |
| BE-05 | `docs/backend/05_Backend_Deployment.md`       |
| CO-01 | `docs/compute/01_Compute_Architecture.md`     |
| CO-02 | `docs/compute/02_Compute_Flow.md`             |
| CO-03 | `docs/compute/03_Compute_Standards.md`        |
| CO-04 | `docs/compute/04_Compute_Business_Rules.md`   |
| CO-05 | `docs/compute/05_Compute_Deployment.md`       |
| WK-01 | `docs/worker/01_Worker_Architecture.md`       |
| WK-02 | `docs/worker/02_Worker_Flow.md`               |
| WK-03 | `docs/worker/03_Worker_Standards.md`          |
| WK-04 | `docs/worker/04_Worker_Business_Rules.md`     |
| WK-05 | `docs/worker/05_Worker_Deployment.md`         |
| FE-01 | `docs/frontend/01_Frontend_Architecture.md`   |
| FE-02 | `docs/frontend/02_Frontend_Flow.md`           |
| FE-03 | `docs/frontend/03_Frontend_Standards.md`      |
| FE-04 | `docs/frontend/04_Frontend_Business_Rules.md` |
| FE-05 | `docs/frontend/05_Frontend_Deployment.md`     |
