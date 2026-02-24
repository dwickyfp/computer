# Rosetta ETL Platform - Visual Architecture Diagrams

## System Overview

```
┌────────────────────────────────────────────────────────────────┐
│                         CLIENTS                                 │
│  (Web Browser, API Client, Mobile App, CLI Tool)              │
└───────────────────────────┬────────────────────────────────────┘
                            │
                            │ HTTP/HTTPS
                            ↓
┌────────────────────────────────────────────────────────────────┐
│                     FASTAPI APPLICATION                         │
│                  (Rosetta ETL Platform)                        │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │              API Layer (Presentation)                     │ │
│  │  • Sources Endpoints     • Pipelines Endpoints           │ │
│  │  • Destinations Endpoints • Metrics Endpoints            │ │
│  │  • Health Checks         • OpenAPI Documentation         │ │
│  └───────────────────────┬──────────────────────────────────┘ │
│                          │                                     │
│  ┌───────────────────────┴──────────────────────────────────┐ │
│  │            Service Layer (Application)                    │ │
│  │  • SourceService        • PipelineService                │ │
│  │  • DestinationService   • WALMonitorService              │ │
│  └───────────────────────┬──────────────────────────────────┘ │
│                          │                                     │
│  ┌───────────────────────┴──────────────────────────────────┐ │
│  │            Domain Layer                                   │ │
│  │  Models:  Source, Destination, Pipeline, WALMetric       │ │
│  │  Schemas: Pydantic validation models                     │ │
│  └───────────────────────┬──────────────────────────────────┘ │
│                          │                                     │
│  ┌───────────────────────┴──────────────────────────────────┐ │
│  │         Repository Layer (Data Access)                    │ │
│  │  • BaseRepository (Generic CRUD)                         │ │
│  │  • SourceRepository  • DestinationRepository             │ │
│  │  • PipelineRepository • WALMetricRepository              │ │
│  └───────────────────────┬──────────────────────────────────┘ │
│                          │                                     │
│  ┌───────────────────────┴──────────────────────────────────┐ │
│  │      Infrastructure Layer                                 │ │
│  │  • Database Connection Pool                              │ │
│  │  • Background Task Scheduler                             │ │
│  │  • Logging & Monitoring                                  │ │
│  └──────────────────────────────────────────────────────────┘ │
└────────────────────────────┬───────────────────────────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                         │
        ↓                                         ↓
┌───────────────────┐                  ┌──────────────────────┐
│  PostgreSQL DB    │                  │  PostgreSQL Sources  │
│  (Metadata)       │                  │  (Monitored DBs)     │
│                   │                  │                      │
│  • sources        │                  │  • WAL monitoring    │
│  • destinations   │                  │  • Replication data  │
│  • pipelines      │                  │                      │
│  • wal_metrics    │                  └──────────────────────┘
└───────────────────┘
```

## Clean Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXTERNAL INTERFACES                           │
│  HTTP, CLI, Message Queue, etc.                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│              INTERFACE ADAPTERS (Controllers)                    │
│                                                                  │
│  app/api/v1/endpoints/                                          │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐           │
│  │   sources.py │ │destinations.py│ │ pipelines.py │           │
│  └──────────────┘ └──────────────┘ └──────────────┘           │
│                                                                  │
│  Responsibility: HTTP request/response, validation              │
│  Dependencies: → Services                                       │
└────────────────────────┬────────────────────────────────────────┘
                         │ Dependency Rule: Points Inward
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│              USE CASES (Business Logic)                          │
│                                                                  │
│  app/domain/services/                                           │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐           │
│  │  source.py   │ │destination.py│ │ pipeline.py  │           │
│  └──────────────┘ └──────────────┘ └──────────────┘           │
│                                                                  │
│  Responsibility: Orchestrate business operations                │
│  Dependencies: → Repositories, Domain Models                    │
└────────────────────────┬────────────────────────────────────────┘
                         │ Dependency Rule: Points Inward
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│                 ENTITIES (Domain Models)                         │
│                                                                  │
│  app/domain/models/                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐           │
│  │  source.py   │ │destination.py│ │ pipeline.py  │           │
│  └──────────────┘ └──────────────┘ └──────────────┘           │
│                                                                  │
│  Responsibility: Core business rules and data structures        │
│  Dependencies: None (Framework-independent)                     │
└─────────────────────────────────────────────────────────────────┘
                         ↑
                         │ Dependency Inversion
                         │
┌─────────────────────────────────────────────────────────────────┐
│           FRAMEWORKS & DRIVERS (Infrastructure)                  │
│                                                                  │
│  app/domain/repositories/ & app/infrastructure/                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐           │
│  │   Database   │ │ Background   │ │   External   │           │
│  │  Repository  │ │    Tasks     │ │   Services   │           │
│  └──────────────┘ └──────────────┘ └──────────────┘           │
│                                                                  │
│  Responsibility: External I/O, database, file system            │
│  Dependencies: → Domain Interfaces (via Dependency Inversion)   │
└─────────────────────────────────────────────────────────────────┘
```

## Request Flow Diagram

```
1. HTTP Request
   │
   ├─→ [FastAPI Route Handler]
   │    ↓
   │   Validate with Pydantic Schema
   │    ↓
   ├─→ [Dependency Injection]
   │    ↓
   │   Get Database Session
   │   Create Service Instance
   │    ↓
   ├─→ [Service Layer]
   │    ↓
   │   Execute Business Logic
   │   Orchestrate Multiple Operations
   │    ↓
   ├─→ [Repository Layer]
   │    ↓
   │   Construct SQLAlchemy Query
   │   Execute Database Operation
   │    ↓
   ├─→ [Database]
   │    ↓
   │   Return Data
   │    ↓
   ├─→ [Service Layer]
   │    ↓
   │   Apply Business Rules
   │   Transform Data
   │    ↓
   ├─→ [Response Schema]
   │    ↓
   │   Serialize to Pydantic Model
   │   Exclude Sensitive Fields
   │    ↓
   └─→ [HTTP Response]
        ↓
       JSON to Client
```

## Database Connection Pool Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              FastAPI Application (Multiple Workers)          │
│                                                              │
│  Worker 1   Worker 2   Worker 3   Worker 4   ...  Worker N │
│     │          │          │          │                │     │
│     └──────────┴──────────┴──────────┴────────────────┘     │
│                            │                                 │
│                            ↓                                 │
│  ┌──────────────────────────────────────────────────────┐  │
│  │        SQLAlchemy Async Connection Pool              │  │
│  │                                                       │  │
│  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐  ...  ┌────┐  │  │
│  │  │ C1 │ │ C2 │ │ C3 │ │ C4 │ │ C5 │       │ C20│  │  │
│  │  └────┘ └────┘ └────┘ └────┘ └────┘  ...  └────┘  │  │
│  │                                                       │  │
│  │  Pool Size: 20 connections                          │  │
│  │  Strategy: LIFO (Last In, First Out)                │  │
│  │  Pre-Ping: Enabled                                   │  │
│  │  Recycle: 3600 seconds                              │  │
│  │                                                       │  │
│  │  ┌────┐ ┌────┐ ┌────┐                              │  │
│  │  │ O1 │ │ O2 │ │ O3 │  ... (Max Overflow: 10)      │  │
│  │  └────┘ └────┘ └────┘                              │  │
│  │                                                       │  │
│  │  Timeout: 30 seconds                                 │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                 PostgreSQL Database                          │
│                                                              │
│  • Metadata tables (sources, destinations, pipelines)       │
│  • WAL metrics history                                      │
│  • Transaction management                                   │
└─────────────────────────────────────────────────────────────┘
```

## WAL Monitoring Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    APScheduler                               │
│   Triggers every WAL_MONITOR_INTERVAL_SECONDS (default: 300)│
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│              WALMonitorService.monitor_all_sources()         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│         Fetch All Sources from Database                      │
│         (SourceRepository.get_all())                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│      Concurrent Monitoring (asyncio.gather)                  │
│                                                              │
│   ┌────────────┐  ┌────────────┐  ┌────────────┐          │
│   │ Source 1   │  │ Source 2   │  │ Source N   │          │
│   └─────┬──────┘  └─────┬──────┘  └─────┬──────┘          │
│         │                │                │                 │
│         ↓                ↓                ↓                 │
│   ┌─────────────────────────────────────────┐              │
│   │  WALMonitorService.monitor_source()     │              │
│   │                                          │              │
│   │  1. Connect to source database          │              │
│   │     (asyncpg.connect)                   │              │
│   │                                          │              │
│   │  2. Execute WAL query:                  │              │
│   │     SELECT pg_wal_lsn_diff(            │              │
│   │       pg_current_wal_lsn(), '0/0'      │              │
│   │     )::bigint AS wal_size_bytes;       │              │
│   │                                          │              │
│   │  3. Calculate size                      │              │
│   │                                          │              │
│   │  4. Persist to wal_metrics              │              │
│   │     (WALMetricRepository.record_metric) │              │
│   │                                          │              │
│   │  5. Retry on failure (max 3 times)     │              │
│   │     Exponential backoff: 2^retry_count  │              │
│   └─────────────────────────────────────────┘              │
└─────────────────────────────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│              Metadata Database                               │
│                                                              │
│  INSERT INTO wal_metrics (source_id, size_bytes,            │
│                           recorded_at)                       │
│  VALUES (?, ?, NOW())                                       │
└─────────────────────────────────────────────────────────────┘
```

## Pipeline Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                    Pipeline Creation                         │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  1. POST /api/v1/pipelines                                   │
│     Request Body: {                                          │
│       name, source_id, destination_id, status                │
│     }                                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  2. PipelineService.create_pipeline()                        │
│                                                              │
│     ┌───────────────────────────────────────┐              │
│     │ Validate source exists                │              │
│     │   (SourceRepository.get_by_id)        │              │
│     └───────────────┬───────────────────────┘              │
│                     │                                        │
│     ┌───────────────▼───────────────────────┐              │
│     │ Validate destination exists           │              │
│     │   (DestinationRepository.get_by_id)   │              │
│     └───────────────┬───────────────────────┘              │
│                     │                                        │
│     ┌───────────────▼───────────────────────┐              │
│     │ Create Pipeline entity                │              │
│     │   (PipelineRepository.create)         │              │
│     └───────────────┬───────────────────────┘              │
│                     │                                        │
│     ┌───────────────▼───────────────────────┐              │
│     │ Create PipelineMetadata               │              │
│     │   status = RUNNING                    │              │
│     └───────────────┬───────────────────────┘              │
│                     │                                        │
│     ┌───────────────▼───────────────────────┐              │
│     │ Commit transaction                    │              │
│     └───────────────┬───────────────────────┘              │
│                     │                                        │
│     ┌───────────────▼───────────────────────┐              │
│     │ Load relationships (source,           │              │
│     │   destination, metadata)              │              │
│     └───────────────────────────────────────┘              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│  3. Return PipelineResponse                                  │
│     {                                                        │
│       id, name, status,                                      │
│       source: {...},                                         │
│       destination: {...},                                    │
│       metadata: {...}                                        │
│     }                                                        │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                Pipeline State Transitions                    │
└─────────────────────────────────────────────────────────────┘

        ┌─────────────┐
        │   CREATED   │
        └──────┬──────┘
               │
               ↓
        ┌─────────────┐         POST /pipelines/{id}/pause
        │   START     │◄────────────────────────────────┐
        │  (running)  │                                 │
        └──────┬──────┘                                 │
               │                                        │
               │ POST /pipelines/{id}/pause             │
               ↓                                        │
        ┌─────────────┐                                 │
        │   PAUSE     │                                 │
        │  (paused)   │                                 │
        └──────┬──────┘                                 │
               │                                        │
               │ POST /pipelines/{id}/start             │
               └────────────────────────────────────────┘

                       POST /pipelines/{id}/refresh
                            │
                            ↓
                     ┌─────────────┐
                     │  REFRESH    │
                     │ (refreshing)│
                     └─────────────┘
```

## Repository Pattern Flow

```
┌─────────────────────────────────────────────────────────────┐
│                   BaseRepository<T>                          │
│              (Generic CRUD Operations)                       │
│                                                              │
│  • create(**kwargs) → T                                     │
│  • get_by_id(id) → T                                        │
│  • get_by_name(name) → T | None                            │
│  • get_all(skip, limit) → List[T]                          │
│  • update(id, **kwargs) → T                                │
│  • delete(id) → None                                        │
│  • count() → int                                            │
│  • exists(id) → bool                                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ Inheritance
                         │
     ┌───────────────────┼───────────────────┐
     │                   │                   │
     ↓                   ↓                   ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Source       │  │ Destination  │  │ Pipeline     │
│ Repository   │  │ Repository   │  │ Repository   │
├──────────────┤  ├──────────────┤  ├──────────────┤
│              │  │              │  │              │
│ + get_       │  │ (Uses base   │  │ + get_by_id_ │
│   sources_   │  │  methods)    │  │   with_      │
│   with_wal_  │  │              │  │   relations  │
│   metrics    │  │              │  │              │
│              │  │              │  │ + get_by_    │
│              │  │              │  │   status     │
└──────────────┘  └──────────────┘  └──────────────┘
```

---

**Legend**:

- `→` : Data flow / Dependency
- `↓` : Sequential flow
- `├─→` : Branch in flow
- `┌─┐` : Component/Layer boundary
