# Debezium PostgreSQL Connection Troubleshooting Guide

## Common Error: Publication Does Not Exist

### Symptom

```
org.postgresql.util.PSQLException: ERROR: publication "rosetta_publication" does not exist
  Where: slot "rosetta_replication", output plugin "pgoutput", in the change callback
```

### Root Cause

This error occurs when:

1. A replication slot exists and is configured to use a specific publication
2. The publication was deleted or never created
3. Debezium attempts to read from the replication slot, which references the non-existent publication

### Quick Fix

**Option 1: Use the diagnostic script**

```bash
# Diagnose the issue
python compute/scripts/fix_replication_setup.py --source-id 1

# Automatically fix
python compute/scripts/fix_replication_setup.py --source-id 1 --fix
```

**Option 2: Manual fix via SQL**

```sql
-- Connect to the source database
\c your_database

-- Check if publication exists
SELECT * FROM pg_publication WHERE pubname = 'rosetta_publication';

-- If missing, create it for all tables
CREATE PUBLICATION rosetta_publication FOR ALL TABLES;

-- Or create it for specific tables
CREATE PUBLICATION rosetta_publication FOR TABLE schema.table1, schema.table2;

-- If publication exists but replication slot is orphaned, drop and recreate slot
SELECT pg_drop_replication_slot('rosetta_replication');
-- Debezium will recreate it automatically on next start
```

---

## Optimized Debezium Configuration

The following optimizations have been added to `compute/sources/postgresql.py`:

### 1. Connection Stability

```python
"database.tcpKeepAlive": "true"           # Keep connections alive
"database.connectTimeout": "30000"         # 30s connection timeout
"database.socketTimeout": "60000"          # 60s socket timeout
```

**Why**: Prevents connection drops on slow networks or long-running queries.

### 2. Publication Auto-Create Disabled

```python
"publication.autocreate.mode": "disabled"
```

**Why**: Forces explicit publication creation, preventing automatic creation of publications that may not include all desired tables.

### 3. Error Handling

```python
"errors.max.retries": "3"
"errors.retry.delay.initial.ms": "1000"
"errors.retry.delay.max.ms": "30000"
```

**Why**: Limits retry attempts for unrecoverable errors (like missing publication) rather than retrying infinitely.

### 4. Status Updates

```python
"status.update.interval.ms": "10000"       # Update status every 10s
```

**Why**: Provides more frequent feedback on connector health.

### 5. Tombstone Events

```python
"tombstones.on.delete": "true"
```

**Why**: Emits tombstone events for deleted records, useful for downstream processing.

---

## Pre-Flight Validation

A new validation method `validate_replication_setup()` is called before starting each pipeline:

```python
def validate_replication_setup(self, pipeline_name: str) -> tuple[bool, str]:
    """
    Checks:
    1. Publication exists
    2. Publication has tables
    3. Replication slot status (exists or will be auto-created)

    Returns: (is_valid, error_message)
    """
```

This validation **prevents pipelines from starting** when critical resources are missing, stopping Debezium from infinitely retrying.

---

## PostgreSQL Configuration Requirements

### Required Settings in `postgresql.conf`

```ini
# Replication
wal_level = logical                    # Required for logical replication
max_replication_slots = 10             # At least one per pipeline
max_wal_senders = 10                   # At least one per pipeline

# WAL Management
wal_keep_size = 512MB                  # Retain WAL segments (PG 13+)
# OR for older versions:
wal_keep_segments = 64                 # Retain 64 WAL segments (PG 12 and earlier)
```

**Note**: After changing these settings, restart PostgreSQL:

```bash
# Linux/Mac
sudo systemctl restart postgresql

# Windows
net stop postgresql-x64-14
net start postgresql-x64-14

# Docker
docker restart <container-name>
```

---

## Monitoring Replication Health

### Check Publication Status

```sql
-- List all publications
SELECT * FROM pg_publication;

-- Check tables in publication
SELECT * FROM pg_publication_tables WHERE pubname = 'rosetta_publication';

-- Add table to publication
ALTER PUBLICATION rosetta_publication ADD TABLE schema.table_name;

-- Remove table from publication
ALTER PUBLICATION rosetta_publication DROP TABLE schema.table_name;
```

### Check Replication Slot Status

```sql
-- List all replication slots
SELECT
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size
FROM pg_replication_slots;

-- Check WAL size retained by slot
SELECT
    slot_name,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as wal_retained
FROM pg_replication_slots
WHERE slot_name = 'rosetta_replication';

-- Drop inactive slot
SELECT pg_drop_replication_slot('rosetta_replication');
```

### Check WAL Growth

```sql
-- Current WAL LSN
SELECT pg_current_wal_lsn();

-- WAL size on disk
SELECT
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_generated;
```

---

## Common Issues and Solutions

### Issue 1: Infinite Retry Loop

**Symptom**: Debezium retries infinitely with retriable exception
**Cause**: Missing publication or replication slot references non-existent publication
**Solution**: Use the fix script or manually recreate publication

### Issue 2: WAL Size Growing Indefinitely

**Symptom**: PostgreSQL WAL directory size increases continuously
**Cause**: Replication slot not advancing (inactive or stuck)
**Solution**:

1. Check if pipeline is running: `SELECT active FROM pg_replication_slots WHERE slot_name = 'rosetta_replication';`
2. If inactive, drop slot: `SELECT pg_drop_replication_slot('rosetta_replication');`
3. Restart pipeline to recreate slot

### Issue 3: Connection Timeout

**Symptom**: `connection timeout` or `socket timeout`
**Cause**: Network issues or firewall blocking connections
**Solution**:

1. Verify network connectivity: `psql -h <host> -p <port> -U <user> -d <database>`
2. Check firewall rules for PostgreSQL port
3. Increase timeout values in Debezium config

### Issue 4: Permission Denied

**Symptom**: `permission denied to create replication slot`
**Cause**: Database user lacks replication privileges
**Solution**:

```sql
-- Grant replication privileges
ALTER ROLE your_user REPLICATION;

-- Or use superuser
ALTER ROLE your_user SUPERUSER;
```

---

## Best Practices

### 1. Publication Management

- Create publications **before** starting pipelines
- Use `FOR ALL TABLES` for simplicity or specify tables explicitly
- Don't delete publications while pipelines are running

### 2. Replication Slot Management

- Let Debezium create slots automatically
- Monitor slot lag regularly
- Drop inactive slots to prevent WAL accumulation
- Use unique slot names per pipeline

### 3. Pipeline Lifecycle

- Always validate replication setup before starting
- Monitor pipeline health through `pipeline_metadata` table
- Check logs for connection errors or retries
- Use the `is_publication_enabled` and `is_replication_enabled` flags in source table

### 4. Maintenance Windows

When updating PostgreSQL configuration:

1. Pause all pipelines: `UPDATE pipelines SET status = 'PAUSE'`
2. Wait for compute service to stop all engines
3. Update PostgreSQL configuration
4. Restart PostgreSQL
5. Resume pipelines: `UPDATE pipelines SET status = 'START'`

---

## Debugging Commands

```bash
# Check compute service logs
tail -f compute/logs/compute.log

# Check pipeline status
psql -h localhost -p 5433 -U postgres -d rosetta_config \
  -c "SELECT id, name, status FROM pipelines;"

# Check source configuration
psql -h localhost -p 5433 -U postgres -d rosetta_config \
  -c "SELECT id, name, publication_name, replication_name, is_publication_enabled, is_replication_enabled FROM sources;"

# Check pipeline metadata
psql -h localhost -p 5433 -U postgres -d rosetta_config \
  -c "SELECT pipeline_id, health_status, error_message FROM pipeline_metadata;"
```

---

## Additional Resources

- [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [PostgreSQL Replication Slots](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS)
