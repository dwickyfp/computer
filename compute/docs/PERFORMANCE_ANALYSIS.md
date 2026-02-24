# Compute CDC Performance Analysis
## Scale Requirements: 20+ Pipelines × 100+ Tables

**Analysis Date:** February 11, 2026  
**Target Scale:** 20 pipelines, 100 tables per pipeline = 2,000 total tables  
**Current Architecture:** Multiprocess CDC engine with Debezium

---

## Executive Summary

The current compute CDC architecture **can handle** 20+ pipelines with 100+ tables each, but requires tuning and monitoring. The multiprocess architecture provides excellent fault isolation, but introduces resource management challenges at scale.

### Key Findings

✅ **Strengths:**
- Process isolation prevents cascade failures
- Independent connection pools per pipeline
- Batch processing with configurable sizes
- Automatic DLQ for failed writes

⚠️ **Bottlenecks Identified:**
1. **Database connections:** Up to 110 concurrent connections to config DB
2. **Memory overhead:** 20 Python processes + JVM per process
3. **Replication slots:** 20 PostgreSQL replication slots (WAL accumulation risk)
4. **Polling overhead:** 5-second polling interval for 20 pipelines
5. **Batch size:** Default 2048 may be suboptimal for high-throughput

---

## Architecture Overview

### Current Design

```
Main Process
├── API Server (port 8001)
├── Pipeline Manager (monitors DB every 5s)
├── Backfill Manager (separate thread)
└── Connection Pool (min=1, max=10)

Pipeline Process 1 (multiprocessing)
├── Debezium Engine (blocking)
├── CDC Event Handler
├── DLQ Recovery Worker
├── Connection Pool (min=1, max=5)
└── Destinations (Snowflake/PostgreSQL)

Pipeline Process 2...20 (same structure)
```

### Data Flow Per Pipeline

```
PostgreSQL WAL
  ↓ (pgoutput replication)
Debezium Engine
  ↓ (batch of ChangeEvents)
CDCEventHandler.handleJsonBatch()
  ↓ (groups by table_name)
_process_table_records()
  ↓ (routes to destinations)
destination.write_batch()
  ↓ (on failure)
DLQ Manager (Redis Streams)
```

---

## Resource Consumption Analysis

### 1. Database Connections

**Configuration:**
```python
# Main process (manager.py, backfill_manager.py)
init_connection_pool(min_conn=1, max_conn=10)  # 1 pool

# Each pipeline process (_run_pipeline_process)
init_connection_pool(min_conn=1, max_conn=5)   # 20 pools
```

**Total Connections to Config DB:**
- Main process: 1-10 connections
- 20 pipeline processes: 20-100 connections
- **Peak Total: 110 connections**

**PostgreSQL Default:** `max_connections = 100`

**⚠️ RISK:** Default PostgreSQL config cannot handle peak load.

**Recommendation:**
```sql
-- On config database
ALTER SYSTEM SET max_connections = 200;
SELECT pg_reload_conf();

-- Or optimize connection usage
# In main.py
init_connection_pool(min_conn=1, max_conn=5)  # Reduce main pool

# In manager.py line 67
init_connection_pool(min_conn=1, max_conn=3)  # Reduce per-pipeline
```

### 2. Memory Footprint

**Per Pipeline Process:**
- Python interpreter: ~30-50 MB
- JVM (Debezium): ~256-512 MB (depends on `max.batch.size`)
- pydbzengine overhead: ~50-100 MB
- Routing table for 100 tables: ~5-10 MB
- **Estimated: 400-700 MB per pipeline**

**Total for 20 Pipelines: 8-14 GB**

**Recommendation:**
- Minimum server RAM: **16 GB**
- Recommended: **32 GB** (with OS overhead, Redis, etc.)

### 3. PostgreSQL Source Overhead

**Per Pipeline:**
- 1 replication slot (`source.replication_name`)
- 1 publication (`source.publication_name`) - shared across pipelines using same source
- WAL retention based on slowest consumer

**For 20 Pipelines:**
- Up to 20 replication slots (if different sources)
- WAL size grows if any pipeline falls behind

**Current Mitigation:**
```python
# sources/postgresql.py line 247
"heartbeat.interval.ms": "10000"  # 10s heartbeats prevent WAL bloat
```

**⚠️ RISK:** If 1 pipeline stops, its replication slot holds WAL indefinitely.

**Monitoring Query:**
```sql
-- Check replication slot lag
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE slot_name LIKE 'rosetta_%'
ORDER BY retained_wal DESC;
```

**Recommendation:**
```python
# Add to sources/postgresql.py build_debezium_props()
"slot.drop.on.stop": "false",  # Keep for restart
"slot.max.retries": "6",       # Already configured
"slot.retry.delay.ms": "10000" # Already configured

# Consider WAL monitoring alert
# When retained_wal > 10GB, alert operators
```

---

## Performance Tuning Recommendations

### 1. Debezium Batch Configuration

**Current Settings (config/config.py):**
```python
max_batch_size: int = 2048      # Records per batch
max_queue_size: int = 8192      # Internal queue size
poll_interval_ms: int = 500     # Polling frequency
```

**For High-Throughput (100+ tables):**
```python
# Increase batch size to reduce per-batch overhead
max_batch_size: int = 4096      # or 8192

# Increase queue for bursty traffic
max_queue_size: int = 16384

# Reduce polling frequency to lower CPU
poll_interval_ms: int = 1000    # 1s instead of 500ms
```

**Environment Variables:**
```bash
export PIPELINE_MAX_BATCH_SIZE=4096
export PIPELINE_MAX_QUEUE_SIZE=16384
export PIPELINE_POLL_INTERVAL_MS=1000
```

**Trade-off:** Larger batches = higher throughput but increased memory and latency.

### 2. Pipeline Manager Polling

**Current (manager.py line 357):**
```python
def monitor(self, check_interval: float = 5.0) -> None:
    while not self._shutdown_event.is_set():
        self._sync_pipelines_state()  # Queries DB for all 20 pipelines
        time.sleep(check_interval)
```

**For 20 Pipelines:**
```python
# Increase interval to reduce DB load
def monitor(self, check_interval: float = 10.0) -> None:  # 10s
```

**Impact:** Status changes (START/PAUSE) take up to 10s to take effect instead of 5s.

### 3. Connection Pool Optimization

**Current Issues:**
- Every DB query gets connection from pool
- 20 pipeline processes holding connections idle during low traffic
- Main process backfill manager queries DB frequently

**Optimization:**

```python
# In manager.py line 67 - reduce per-pipeline pool
init_connection_pool(min_conn=1, max_conn=3)  # Down from 5

# In main.py line 108 - reduce main pool
init_connection_pool(min_conn=2, max_conn=8)  # Down from 10
```

**Revised Total Connections:**
- Main: 2-8 connections
- 20 pipelines: 20-60 connections
- **Peak Total: 68 connections** ✅ Fits in default PostgreSQL config

### 4. Event Handler Routing Table

**Current (event_handler.py line 75-107):** 
Builds routing table on initialization for each pipeline.

**For 100 Tables:**
```python
self._routing_table: dict[str, list[RoutingInfo]]
# Structure: {"table_name": [RoutingInfo(destination1), RoutingInfo(destination2)]}
```

**Memory per pipeline:** ~5-10 MB for 100 tables × average 2 destinations

**✅ No optimization needed** - routing table is efficient dict lookup.

### 5. Batch Write Optimization

**Current (event_handler.py line 226):**
```python
def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
    # Groups by table
    records_by_table: dict[str, list[CDCRecord]] = {}
    
    # Processes each table sequentially
    for table_name, table_records in records_by_table.items():
        self._process_table_records(table_name, table_records)
```

**⚠️ BOTTLENECK:** 100 tables processed sequentially in single pipeline.

**Potential Optimization (requires testing):**
```python
from concurrent.futures import ThreadPoolExecutor

def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
    records_by_table = self._group_records_by_table(records)
    
    # Process tables in parallel (max 4 concurrent)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for table_name, table_records in records_by_table.items():
            future = executor.submit(
                self._process_table_records, 
                table_name, 
                table_records
            )
            futures.append(future)
        
        # Wait for all to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                self._logger.error(f"Table processing failed: {e}")
```

**Trade-offs:**
- ✅ Faster processing for multi-table batches
- ⚠️ Increased connection pool usage (need higher max_conn)
- ⚠️ Complex error handling

---

## Monitoring & Alerting

### Critical Metrics to Track

```python
# 1. Pipeline Process Health
# Check: ps aux | grep "Pipeline_" | wc -l
# Expected: 20 (one per pipeline)

# 2. Database Connections
SELECT count(*) FROM pg_stat_activity 
WHERE application_name LIKE 'rosetta%';
# Expected: < 70 with optimized pools

# 3. Replication Slot Lag (per source)
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'rosetta_%';
# Alert if lag > 5 GB

# 4. DLQ Depth (Redis)
redis-cli XLEN rosetta:dlq:pipeline_{pipeline_id}:destination_{dest_id}:table_{table}
# Alert if > 10,000 messages

# 5. Memory Usage
ps aux | grep "Pipeline_" | awk '{sum+=$6} END {print sum/1024 " MB"}'
# Expected: 8,000-14,000 MB for 20 pipelines

# 6. CPU Usage
top -b -n 1 | grep "Pipeline_" | awk '{sum+=$9} END {print sum "%"}'
# Expected: 200-400% (2-4 cores) during active replication
```

### Recommended Alerts

```yaml
alerts:
  - name: pipeline_process_down
    condition: active_pipeline_count < expected_count
    threshold: 5 minutes
    
  - name: replication_lag_high
    condition: replication_slot_lag > 5GB
    threshold: 10 minutes
    
  - name: dlq_backlog
    condition: dlq_message_count > 10000
    threshold: 15 minutes
    
  - name: connection_pool_exhausted
    condition: pool_wait_count > 100
    threshold: 5 minutes
    
  - name: memory_high
    condition: total_memory_mb > 20000
    threshold: 30 minutes
```

---

## Load Testing Recommendations

### Test Scenarios

**Scenario 1: Steady State**
- 20 pipelines active
- 50 inserts/sec per table
- 100 tables × 50 ops/sec = 5,000 CDC events/sec total per pipeline
- **Expected:** <2s end-to-end latency, <50% CPU, <12GB RAM

**Scenario 2: Burst Traffic**
- Bulk insert 10,000 rows across 10 tables simultaneously
- **Expected:** Debezium batches into 4096-record chunks, <30s completion

**Scenario 3: Destination Failure**
- Kill Snowflake connection for 1 pipeline
- **Expected:** DLQ accumulates, recovery worker retries, no data loss

**Scenario 4: Pipeline Restart**
- Restart 5 pipelines simultaneously  
- **Expected:** <15s restart time, no duplicate records (offset tracking)

### Load Test Script

```python
# tests/load_test.py
import asyncio
import psycopg2
from concurrent.futures import ThreadPoolExecutor

def generate_load(source_conn_string, num_tables=100, ops_per_sec=50):
    """Generate synthetic CDC traffic."""
    conn = psycopg2.connect(source_conn_string)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        for table_num in range(num_tables):
            executor.submit(
                insert_rows,
                conn,
                f"test_table_{table_num}",
                ops_per_sec
            )

def insert_rows(conn, table_name, rate):
    """Insert rows at specified rate."""
    import time
    interval = 1.0 / rate
    
    while True:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {table_name} (data) VALUES (%s)",
                (f"test_data_{time.time()}",)
            )
            conn.commit()
        time.sleep(interval)
```

---

## Scaling Beyond 20 Pipelines

### Architecture Options

**Option 1: Horizontal Scaling (Multiple Compute Instances)**
```
Load Balancer
├── Compute Instance 1 (pipelines 1-10)
├── Compute Instance 2 (pipelines 11-20)
└── Compute Instance 3 (pipelines 21-30)
```

**Implementation:**
```python
# main.py - filter pipelines by instance ID
INSTANCE_ID = int(os.getenv("COMPUTE_INSTANCE_ID", "1"))
TOTAL_INSTANCES = int(os.getenv("TOTAL_COMPUTE_INSTANCES", "1"))

def _sync_pipelines_state(self) -> None:
    db_pipelines = PipelineRepository.get_all()
    
    # Distribute pipelines across instances
    my_pipelines = [
        p for p in db_pipelines 
        if p.id % TOTAL_INSTANCES == (INSTANCE_ID - 1)
    ]
    
    # Process only assigned pipelines
    ...
```

**Option 2: Shared JVM Optimization**
- Current: Each pipeline spawns separate JVM (Debezium engine)
- Alternative: Use Kafka Connect distributed mode (complex setup)

**Option 3: Thread-Based Instead of Process-Based**
- Replace `multiprocessing.Process` with `threading.Thread`
- ✅ Lower memory (shared interpreter)
- ⚠️ GIL contention for CPU-bound operations
- ⚠️ No fault isolation (one crash kills all)

---

## Implementation Checklist

### Phase 1: Pre-Production (1 week)
- [ ] Increase config DB `max_connections` to 200
- [ ] Tune batch sizes via environment variables
- [ ] Add replication slot monitoring query
- [ ] Set up memory/CPU monitoring dashboard
- [ ] Reduce connection pool sizes (test with 5 pipelines)

### Phase 2: Load Testing (1 week)
- [ ] Deploy test environment with 20 pipelines
- [ ] Run steady-state test (50 ops/sec × 100 tables)
- [ ] Run burst test (10K row bulk insert)
- [ ] Simulate destination failure scenario
- [ ] Measure end-to-end latency and resource usage

### Phase 3: Production Deployment (1 week)
- [ ] Deploy optimized configuration
- [ ] Monitor for 48 hours with <5 pipelines
- [ ] Gradually scale to 20 pipelines
- [ ] Set up alerting for critical metrics
- [ ] Document runbook for common issues

### Phase 4: Optimization (Ongoing)
- [ ] Profile hot paths with cProfile
- [ ] Consider parallel table processing in event handler
- [ ] Evaluate Redis cluster for DLQ if needed
- [ ] Add metrics export (Prometheus/Grafana)

---

## Conclusion

**Can the system handle 20 pipelines × 100 tables?**
**YES**, with the following conditions:

1. ✅ **Sufficient hardware:** 32GB RAM, 8 CPU cores
2. ✅ **Tuned configuration:** Connection pools, batch sizes, polling intervals
3. ✅ **Monitoring in place:** Replication lag, DLQ depth, memory usage
4. ✅ **Documented recovery procedures:** Restart failed pipelines, drain DLQ

**Estimated Performance:**
- **Throughput:** 5,000-10,000 CDC events/sec per pipeline
- **Latency:** 1-3 seconds end-to-end (WAL → Destination)
- **Memory:** 12-16 GB total (with overhead)
- **CPU:** 300-500% (3-5 cores) under load

**Next Steps:**
1. Apply connection pool optimizations (immediate)
2. Run load tests in staging environment (1 week)
3. Deploy to production with gradual rollout (2 weeks)

---

## Appendix: Quick Reference

### Environment Variables for Tuning

```bash
# Connection Pools
export MAIN_POOL_MAX_CONN=8
export PIPELINE_POOL_MAX_CONN=3

# Debezium Engine
export PIPELINE_MAX_BATCH_SIZE=4096
export PIPELINE_MAX_QUEUE_SIZE=16384
export PIPELINE_POLL_INTERVAL_MS=1000

# Pipeline Manager
export PIPELINE_CHECK_INTERVAL=10

# Database
export ROSETTA_DB_HOST=localhost
export ROSETTA_DB_PORT=5432
export ROSETTA_DB_NAME=rosetta
```

### Key Files to Monitor

```
compute/
├── main.py                    # Connection pool init (line 108)
├── core/
│   ├── manager.py            # Process spawning (line 67, 164)
│   ├── engine.py             # Pipeline lifecycle
│   ├── event_handler.py      # Batch processing (line 226)
│   └── database.py           # Pool configuration (line 23)
├── config/config.py          # Global settings (line 62-73)
└── sources/postgresql.py     # Debezium props (line 247-254)
```

### Useful Debug Commands

```bash
# List all pipeline processes
ps aux | grep "Pipeline_"

# Check connection count
psql -d rosetta -c "SELECT count(*), application_name FROM pg_stat_activity GROUP BY application_name;"

# Monitor CPU per process
top -b -n 1 | grep Pipeline

# Check replication slots
psql -d source_db -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;"

# View DLQ depth
redis-cli --scan --pattern "rosetta:dlq:*" | xargs -I {} redis-cli XLEN {}
```
