# CO-02 — Compute: Application Flow

## 1. Startup Sequence

```
main.py
  │
  ├─ 1. Setup logging (stdout, structured)
  ├─ 2. run_migration()  ──▶  migrations/001_create_table.sql (idempotent DDL)
  ├─ 3. init_connection_pool(min=1, max=5)  ──▶  parent process pool
  │
  ├─ 4. (if CHAIN_ENABLED=true)
  │       run_server()  ──▶  FastAPI on SERVER_HOST:SERVER_PORT (background thread)
  │       ChainCleanupThread  ──▶  trims rosetta:chain:* streams on schedule
  │
  ├─ 5. BackfillManager.start()  ──▶  polling thread (every check_interval seconds)
  │
  ├─ 6. PipelineManager.start()  ──▶  main polling loop (every ~10 s)
  │
  └─ 7. Signal handlers: SIGINT / SIGTERM  ──▶  stop_event.set(), cleanup, exit
```

---

## 2. PipelineManager Poll Loop

```
Every 10 s:
  │
  ├─ 1. Query Config DB: SELECT pipelines WHERE status IN ('START', 'REFRESH')
  │
  ├─ 2. For each pipeline in DB with status=START:
  │       ├─ Already running? → check process.is_alive()
  │       │     alive: compare pipeline.updated_at vs process.last_updated_at
  │       │             (config changed?) → if changed: trigger restart
  │       │     dead:   restart process (log warning)
  │       └─ Not running: spawn new Process
  │
  ├─ 3. For each pipeline in DB with status=REFRESH:
  │       ├─ If running: stop process (send stop_event, join with timeout)
  │       └─ start fresh process (config re-read from DB)
  │
  ├─ 4. For each tracked process no longer in DB (pipeline deleted/paused):
  │       └─ Stop process gracefully (send stop_event, join with timeout)
  │
  └─ 5. sleep(10)
```

---

## 3. Pipeline Process Lifecycle

```
_run_pipeline_process(pipeline_id, stop_event)   [child OS process]
  │
  ├─ 1. init_connection_pool(min=2, max=PIPELINE_POOL_MAX_CONN=20)
  ├─ 2. PipelineRepository.get_by_id(pipeline_id)
  │       not found? → log warning, return (graceful exit)
  │
  ├─ 3. Determine engine type:
  │       source_type='POSTGRES'  → PipelineEngine
  │       source_type='ROSETTA'   → ChainPipelineEngine
  │
  ├─ 4. engine.run()  ──▶  blocking loop
  │       PipelineEngine:      Debezium JVM loop → CDCEventHandler callbacks
  │       ChainPipelineEngine: Redis XREADGROUP loop → CDCEventHandler callbacks
  │
  ├─ 5. stop_event monitors:
  │       PipelineEngine:  checks stop_event every N events; drain and exit
  │       ChainEngine:     XREADGROUP with timeout; checks stop_event after each read
  │
  └─ 6. cleanup:
          close destinations
          close_connection_pool()
          DLQRecoveryWorker.stop()
```

---

## 4. CDC Event Handler Flow

When Debezium fires a change event or ChainEngine reads a record from Redis:

```
CDCEventHandler.handle_event(ChangeEvent)
  │
  ├─ 1. Parse JSON payload (Debezium format):
  │       extract: operation (c/u/d/r), table_name, before, after columns
  │
  ├─ 2. Lookup routing table: table_name → list[RoutingInfo]
  │       (built once at engine start from pipeline.destinations config)
  │
  ├─ 3. For each RoutingInfo (pipeline_destination × table_sync):
  │       ├─ Apply custom SQL filter? → evaluate filter_sql against row
  │       ├─ Apply column mapping? → remap column names
  │       │
  │       ├─ destination.write(CDCRecord)
  │       │     SUCCESS: continue
  │       │     FAILURE: DLQManager.push(record)
  │       │
  │       └─ Update DataFlowRepository (rows_inserted/updated/deleted counters)
  │
  └─ 4. Batch commit (destinations flush on watermark interval)
```

---

## 5. Destination Write Flows

### 5.1 PostgreSQL Destination

```
CDCRecord batch accumulates in memory
  │
  ◀─ On watermark / batch_size reached:
  │
  DuckDB (in-process, per destination instance)
    │  ATTACH PostgreSQL target (via duckdb_postgres extension)
    │
    ├─ operation='r' or 'c':  MERGE INTO target USING staging ON pk
    ├─ operation='u':          MERGE INTO (update matching rows)
    └─ operation='d':          DELETE FROM target WHERE pk = ?
```

DuckDB acts as a transformation and staging layer. The actual write is a DuckDB `MERGE INTO` executed against the remote PostgreSQL via the `postgres` extension.

### 5.2 Snowflake Destination

```
CDCRecord batch
  │
  Snowpipe Streaming REST API  (no Snowflake Connector SDK)
    │  JWT signed with RSA private key (PKCS#8)
    │  POST /v1/channels/{channel}/insertRows
    │
    └─ Async ACK from Snowflake; retry on transient failures
```

### 5.3 Rosetta (Chain) Destination

```
CDCRecord batch
  │
  Serialise to Apache Arrow RecordBatch
    │  pyarrow.serialize_to_stream() → bytes
    │
  HTTP POST to {remote_rosetta_url}/chain/ingest
    │  Headers: X-Chain-Key, X-Table-Name, X-Operation-Type
    │
  Remote Rosetta Compute receives, writes to Redis Stream
```

---

## 6. Backfill Flow

```
BackfillManager (background thread, polls every 5 s)
  │
  ├─ SELECT * FROM queue_backfill_data WHERE status='PENDING' LIMIT 1 FOR UPDATE
  │
  ├─ Mark job: status='EXECUTING'
  │
  ├─ DuckDB:
  │     ATTACH source PostgreSQL (postgres extension)
  │     ATTACH target PostgreSQL (postgres extension)
  │     SELECT * FROM source.table WHERE pk > last_pk_value ORDER BY pk LIMIT batch_size
  │       → write batch to target via MERGE INTO
  │       → update last_pk_value checkpoint
  │     Repeat until no rows left
  │
  ├─ Mark job: status='COMPLETED'
  │
  └─ On failure:
       └─ increment resume_attempts
           < MAX_RESUME_ATTEMPTS? → status='PENDING' (retry next cycle)
           >= MAX_RESUME_ATTEMPTS? → status='FAILED'
```

---

## 7. Rosetta Chain Ingest Flow (Inbound)

```
Remote Rosetta Compute  ──POST /chain/ingest──▶  Chain HTTP Server
                                                    │
                                         chain/auth.py: validate X-Chain-Key
                                                    │
                                         ChainIngestManager.ingest_arrow_ipc()
                                                    │
                            pyarrow.ipc.open_stream(body)
                            iterate RecordBatches
                                                    │
                            for each row → XADD rosetta:chain:{chain_id}:{table}
                                                    │
                            XLEN check → XTRIM at max_stream_length
                                                    │
                            return {"ingested": N}  ──▶  HTTP 200
```

---

## 8. Chain Pipeline Engine Flow (Outbound Consumer)

```
ChainPipelineEngine for pipeline_id X, chain_client_id Y
  │
  ├─ Create consumer group: XGROUP CREATE rosetta:chain:Y:* cg_pipeline_X $ MKSTREAM
  │
  └─ Loop:
       XREADGROUP GROUP cg_pipeline_X consumer_0
                  STREAMS rosetta:chain:Y:{each_subscribed_table}
                  COUNT 500 BLOCK 2000ms
         │
         ├─ Parse stream entries back to CDCRecord
         ├─ route through CDCEventHandler (same as standard CDC)
         ├─ XACK (after successful write)
         └─ check stop_event between reads
```
