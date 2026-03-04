# CO-01 — Compute: Architecture

## 1. Overview

The Compute service is the **CDC (Change Data Capture) engine** of the Rosetta platform. It is responsible for:

1. Reading changes from PostgreSQL sources via **Debezium** (using the `pydbzengine` library which embeds a JVM).
2. Spawning an **isolated OS process** per active pipeline.
3. Routing captured records to one or more **destinations** (Snowflake, PostgreSQL, or a peer Rosetta instance).
4. Managing **backfill** (historical data replication) via DuckDB.
5. Exposing a **Chain Ingest HTTP server** to receive Arrow IPC data from remote Rosetta instances.
6. Providing a **Health API** at `:8001`.

The Compute service deliberately uses no async framework. All concurrency is handled with `multiprocessing`, `threading`, and careful process isolation.

---

## 2. High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  Compute Process  (main.py)                                         │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  PipelineManager  (core/manager.py)                          │   │
│  │  polls Config DB every 10 s                                  │   │
│  │  ┌─────────────────┐  ┌──────────────────┐                  │   │
│  │  │ PipelineProcess │  │  PipelineProcess  │  ...            │   │
│  │  │  (OS Process)   │  │   (OS Process)    │                 │   │
│  │  │                 │  │                   │                 │   │
│  │  │ PipelineEngine  │  │ ChainPipelineEngine│                │   │
│  │  │ (Debezium JVM)  │  │ (Redis XREADGROUP) │                │   │
│  │  │   │             │  │   │                │                │   │
│  │  │   ▼             │  │   ▼                │                │   │
│  │  │ CDCEventHandler │  │ CDCEventHandler    │                │   │
│  │  │   │             │  │   │                │                │   │
│  │  │   ▼             │  │   ▼                │                │   │
│  │  │ Destinations    │  │  Destinations      │                │   │
│  │  └─────────────────┘  └──────────────────┘                  │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────┐  ┌────────────────────────────────────┐  │
│  │  BackfillManager     │  │  Chain HTTP Server (server.py)      │  │
│  │  polls queue every 5s│  │  /chain/ingest                      │  │
│  │  DuckDB PG scanner   │  │  /chain/schema                      │  │
│  └──────────────────────┘  │  /chain/health                      │  │
│                             └────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────┐                                            │
│  │  Chain Cleanup Thread│  trims Redis Streams on retention schedule │
│  └──────────────────────┘                                            │
└────────────────────────────────────────────────────────────────────┘
```

---

## 3. Module Inventory

### 3.1 `core/` — Pipeline Lifecycle

| File                  | Responsibility                                                     |
| --------------------- | ------------------------------------------------------------------ |
| `manager.py`          | `PipelineManager` — orchestrates all pipeline processes            |
| `engine.py`           | `PipelineEngine` — wraps Debezium CDC engine per pipeline          |
| `chain_engine.py`     | `ChainPipelineEngine` — Redis Stream consumer for ROSETTA sources  |
| `event_handler.py`    | `CDCEventHandler` — parses Debezium events, routes to destinations |
| `backfill_manager.py` | `BackfillManager` — DuckDB-based historical data replication       |
| `repository.py`       | Direct psycopg2 queries (no ORM) against Config DB                 |
| `models.py`           | Python dataclasses representing DB rows                            |
| `dlq_manager.py`      | Dead-Letter Queue writes to Redis Streams                          |
| `dlq_recovery.py`     | DLQ retry worker thread per pipeline process                       |
| `schema_validator.py` | Column type compatibility checks                                   |
| `notification.py`     | Notification log writes                                            |
| `security.py`         | Mirrors backend AES-256-GCM encrypt/decrypt                        |
| `error_sanitizer.py`  | Strips credentials from error strings before DB write              |
| `timezone.py`         | UTC+7 timestamp helpers                                            |
| `database.py`         | psycopg2 connection pool (`ThreadedConnectionPool`)                |
| `exceptions.py`       | Compute-specific exception classes                                 |

### 3.2 `destinations/` — Sink Adapters

| Module                       | Destination           | Mechanism                                  |
| ---------------------------- | --------------------- | ------------------------------------------ |
| `destinations/snowflake/`    | Snowflake             | Snowpipe Streaming REST API + JWT (no SDK) |
| `destinations/postgresql.py` | PostgreSQL            | DuckDB `MERGE INTO` + PostgreSQL scanner   |
| `destinations/rosetta.py`    | Peer Rosetta instance | Arrow IPC HTTP POST to `/chain/ingest`     |
| `destinations/base.py`       | Abstract base         | `CDCRecord`, `BaseDestination` interface   |

### 3.3 `sources/` — Source Adapters

| Module                  | Source     | Mechanism                                   |
| ----------------------- | ---------- | ------------------------------------------- |
| `sources/postgresql.py` | PostgreSQL | Debezium connector config builder (via JVM) |
| `sources/base.py`       | Abstract   | `BaseSource` interface                      |

### 3.4 `chain/` — Rosetta Chain (Inbound)

| File        | Responsibility                                                     |
| ----------- | ------------------------------------------------------------------ |
| `ingest.py` | `ChainIngestManager` — deserialises Arrow IPC → Redis XADD         |
| `schema.py` | `ChainSchemaManager` — stores/retrieves table schemas via psycopg2 |
| `auth.py`   | Validates `X-Chain-Key` header                                     |

### 3.5 `server.py` — Chain HTTP Server

FastAPI micro-server started in a background thread:

| Endpoint        | Method | Description                                |
| --------------- | ------ | ------------------------------------------ |
| `/chain/ingest` | POST   | Receive Arrow IPC batch from remote sender |
| `/chain/schema` | POST   | Register table schema from remote sender   |
| `/chain/tables` | GET    | List registered chain tables               |
| `/chain/health` | GET    | Liveness check                             |
| `/health`       | GET    | Compute engine health                      |

---

## 4. Pipeline Engine Types

### Standard CDC — `PipelineEngine`

Uses **Debezium** (via `pydbzengine` Java bridge) to tail the PostgreSQL WAL:

```
PostgreSQL WAL → Debezium (JVM) → CDCEventHandler → Destinations
                 (pydbzengine)      (Python)
```

- Each pipeline gets its own `DebeziumJsonEngine` instance.
- Debezium configuration is built by `PostgreSQLSource.get_config()`.
- Events are serialised as JSON (`DebeziumJsonEngine`) and parsed by `CDCEventHandler`.

### Rosetta Chain — `ChainPipelineEngine`

Reads from Redis Streams instead of Debezium:

```
Redis Stream rosetta:chain:{client_id}:{table}
    → XREADGROUP (consumer group per pipeline)
    → CDCEventHandler (same routing logic)
    → Destinations
```

- No JVM required for chain pipelines.
- Consumer group ID = `pipeline_{pipeline_id}`.
- Auto-acknowledges messages after successful write.

---

## 5. Dead-Letter Queue (DLQ)

Failed destination writes are pushed to Redis Streams:

```
Stream key: dlq:{source_id}:{table_name}:{destination_id}
```

A `DLQRecoveryWorker` thread runs within each pipeline process and retries DLQ records with exponential back-off. Records exceeding max retries are marked as permanently failed.

---

## 6. Connection Pool Strategy

Each pipeline **OS process** initialises its own psycopg2 `ThreadedConnectionPool`:

```python
init_connection_pool(min_conn=2, max_conn=PIPELINE_POOL_MAX_CONN)
# PIPELINE_POOL_MAX_CONN default = 20, configurable via env
```

Connection allocation within each process:

- Engine repository queries: 3–5
- Backfill manager: 3–5
- DLQ recovery worker: 2–3
- Event handler + notification: 2–3
- Pipeline sync + monitoring: 2–3
- Spike buffer: 3–5

The parent `PipelineManager` process has its own separate pool (`min=1, max=5`) used only for polling.

---

## 7. JVM Management

Debezium runs inside a JVM managed by **JPype**. Key considerations:

- The JVM is started once per pipeline process using `_ensure_jvm_started()`.
- `org.jpype.jar` is explicitly included in the JVM classpath to prevent Windows JDK startup failures.
- JVM heap is configurable via `JVM_MAX_HEAP` env var (default `16G`).
- One JVM per process — JVM cannot be forked after start (hence process isolation).
