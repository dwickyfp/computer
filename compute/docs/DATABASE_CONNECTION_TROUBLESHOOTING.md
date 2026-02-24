# Database Connection Troubleshooting Guide

## Common Error: Connection Closed Unexpectedly

### Symptoms

```
psycopg2.DatabaseError: error with status PGRES_TUPLES_OK and no message from the libpq
psycopg2.OperationalError: server closed the connection unexpectedly
```

### Root Causes

1. **Connection Timeout**: Connections in the pool becoming stale
2. **Improper Transaction Handling**: Committing after SELECT queries
3. **Network Issues**: Firewall or network dropping idle connections
4. **PostgreSQL Configuration**: Server closing idle connections too aggressively
5. **Connection Pool Exhaustion**: All connections being used/stuck

---

## Fixes Implemented

### 1. Connection Keepalive Settings

Added TCP keepalive to prevent idle connection drops:

```python
# In database.py
dsn.update({
    'connect_timeout': 10,          # Connection timeout
    'keepalives': 1,                 # Enable TCP keepalive
    'keepalives_idle': 30,           # Wait 30s before sending keepalive
    'keepalives_interval': 10,       # Send keepalive every 10s
    'keepalives_count': 5,           # Try 5 times before giving up
})
```

**Why**: Prevents PostgreSQL and network devices from closing idle connections.

### 2. Connection Validation

Added automatic detection and removal of dead connections:

```python
# Validates connection before use
try:
    conn.isolation_level  # Quick check
except (psycopg2.OperationalError, psycopg2.InterfaceError):
    pool.putconn(conn, close=True)  # Remove dead connection
    conn = pool.getconn()           # Get new one
```

**Why**: Detects and replaces closed connections before they cause errors.

### 3. Smart Transaction Management

Only commits when there are actual write operations:

```python
# Tracks write operations
if query_upper.startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP')):
    self._has_writes = True

# Only commit if writes occurred
if self._conn and not self._autocommit and self._has_writes:
    self._conn.commit()
```

**Why**: Prevents "PGRES_TUPLES_OK" error by avoiding unnecessary commits after SELECT queries.

### 4. Retry Logic

Added automatic retry for transient connection errors:

```python
@retry_on_connection_error(max_retries=3, delay=0.5)
def get_by_id(pipeline_id: int):
    # Repository operation
```

**Why**: Automatically recovers from temporary connection issues without failing the operation.

### 5. Graceful Error Handling

Improved error handling in transaction cleanup:

```python
# In __exit__
try:
    self._conn.commit()
except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
    logger.error(f"Commit failed: {e}")
    connection_valid = False
    raise DatabaseException(f"Commit failed: {e}")

# Close invalid connections instead of returning to pool
if not connection_valid:
    pool.putconn(self._conn, close=True)
```

**Why**: Prevents returning broken connections to the pool, which would cause repeated errors.

---

## PostgreSQL Server Configuration

### Recommended Settings in `postgresql.conf`

```ini
# Connection Settings
max_connections = 100                    # Adjust based on your needs
shared_buffers = 256MB                   # 25% of RAM is a good starting point

# Keepalive Settings (server-side)
tcp_keepalives_idle = 30                 # Match client settings
tcp_keepalives_interval = 10
tcp_keepalives_count = 5

# Connection Lifetime
# Don't set these too low or connections will drop unexpectedly
# idle_in_transaction_session_timeout = 0  # 0 = disabled (recommended)
# statement_timeout = 0                     # 0 = disabled

# Logging (helpful for debugging)
log_connections = on
log_disconnections = on
log_duration = off
log_min_duration_statement = 1000        # Log queries taking >1s
```

**Note**: After changing settings, reload or restart PostgreSQL:

```bash
# Reload config (doesn't require restart)
sudo systemctl reload postgresql

# Or restart
sudo systemctl restart postgresql
```

---

## Connection Pool Configuration

The compute service uses a threaded connection pool with these defaults:

```python
init_connection_pool(min_conn=1, max_conn=10)
```

### Tuning Guidelines

**For Light Load (1-5 pipelines):**

```python
min_conn=1, max_conn=5
```

**For Medium Load (5-20 pipelines):**

```python
min_conn=2, max_conn=20
```

**For Heavy Load (20+ pipelines):**

```python
min_conn=5, max_conn=50
```

**Formula**: `max_conn >= (number_of_pipelines * 2) + 5`

Update in your code or config:

```python
from core.database import init_connection_pool
init_connection_pool(min_conn=2, max_conn=20)
```

---

## Monitoring Connection Health

### Check Active Connections

```sql
-- On config database
SELECT
    count(*) as total_connections,
    usename,
    application_name,
    state
FROM pg_stat_activity
WHERE datname = 'rosetta_config'
GROUP BY usename, application_name, state;
```

### Check Connection Pool Status

```python
from core.database import get_connection_pool

pool = get_connection_pool()
print(f"Min connections: {pool.minconn}")
print(f"Max connections: {pool.maxconn}")
# Note: psycopg2 doesn't expose current usage, but you can infer from pg_stat_activity
```

### Check for Connection Leaks

```sql
-- Long-running idle connections (potential leaks)
SELECT
    pid,
    usename,
    application_name,
    state,
    query,
    state_change,
    NOW() - state_change as idle_time
FROM pg_stat_activity
WHERE state = 'idle'
  AND datname = 'rosetta_config'
  AND NOW() - state_change > INTERVAL '5 minutes'
ORDER BY idle_time DESC;
```

---

## Common Issues and Solutions

### Issue 1: "no message from the libpq"

**Symptom**: `DatabaseError: error with status PGRES_TUPLES_OK and no message from the libpq`
**Cause**: Attempting to commit after a SELECT query or on a closed connection
**Solution**: ✅ Fixed by smart transaction management (only commit on writes)

### Issue 2: "server closed the connection unexpectedly"

**Symptom**: `OperationalError: server closed the connection unexpectedly`
**Cause**: Connection timed out or was killed by PostgreSQL
**Solution**: ✅ Fixed by keepalive settings and connection validation

### Issue 3: Connection Pool Exhausted

**Symptom**: Operations hang or timeout waiting for connections
**Cause**: All connections in use, possibly due to leaks
**Solution**:

1. Check for connection leaks: `SELECT * FROM pg_stat_activity WHERE state = 'idle in transaction'`
2. Increase pool size: `init_connection_pool(max_conn=20)`
3. Ensure all `DatabaseSession` contexts are properly closed

### Issue 4: Transient Connection Errors

**Symptom**: Intermittent connection failures that resolve themselves
**Cause**: Network issues, PostgreSQL restarts, or temporary resource exhaustion
**Solution**: ✅ Fixed by retry logic with exponential backoff

### Issue 5: Multiprocessing Issues

**Symptom**: Errors when using connection pool across processes
**Cause**: Connection pools can't be shared across process boundaries
**Solution**: Initialize a new pool in each process:

```python
# In each spawned process
from core.database import init_connection_pool
init_connection_pool()  # Creates new pool for this process
```

---

## Best Practices

### 1. Always Use Context Managers

```python
# Good ✅
with DatabaseSession() as session:
    session.execute("SELECT * FROM pipelines")
    return session.fetchall()

# Bad ❌
session = DatabaseSession()
session.execute("SELECT * FROM pipelines")
return session.fetchall()  # Connection not returned to pool!
```

### 2. Don't Hold Connections Across I/O

```python
# Bad ❌
with DatabaseSession() as session:
    session.execute("SELECT * FROM pipelines")
    pipelines = session.fetchall()

    for pipeline in pipelines:
        slow_network_call()  # Holding connection during I/O!

# Good ✅
with DatabaseSession() as session:
    session.execute("SELECT * FROM pipelines")
    pipelines = session.fetchall()

for pipeline in pipelines:
    slow_network_call()  # Connection released
```

### 3. Use Retry Decorator for Critical Operations

```python
from core.db_utils import retry_on_connection_error

@retry_on_connection_error(max_retries=3, delay=1.0)
def critical_operation():
    with DatabaseSession() as session:
        session.execute("UPDATE critical_table SET status = 'done'")
        return session.rowcount
```

### 4. Monitor and Log Connection Issues

```python
import logging
logger = logging.getLogger(__name__)

try:
    result = PipelineRepository.get_by_id(pipeline_id)
except DatabaseException as e:
    logger.error(f"Database error: {e}", exc_info=True)
    # Handle or re-raise
```

### 5. Handle Connection Errors in Multiprocessing

```python
from multiprocessing import Process
from core.database import init_connection_pool

def worker_process(pipeline_id):
    # Initialize new pool in child process
    init_connection_pool(min_conn=1, max_conn=5)

    # Now safe to use database
    pipeline = PipelineRepository.get_by_id(pipeline_id)
    # ... work ...

# Spawn process
p = Process(target=worker_process, args=(1,))
p.start()
```

---

## Debugging Commands

### Check Compute Service Connection Status

```bash
# Check if compute service can connect
docker exec rosetta-compute python -c "
from core.database import get_db_connection
conn = get_db_connection()
print('Connection successful:', conn.closed == 0)
conn.close()
"
```

### Test Connection Keepalive

```python
import time
from core.database import get_db_connection, return_db_connection

conn = get_db_connection()
print("Got connection")

# Wait longer than keepalive_idle
time.sleep(60)

# Try to use connection
try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print("Connection still alive!")
except Exception as e:
    print(f"Connection died: {e}")
finally:
    return_db_connection(conn)
```

### Force Connection Pool Reset

```python
from core.database import close_connection_pool, init_connection_pool

# Close all connections
close_connection_pool()

# Recreate pool
init_connection_pool(min_conn=1, max_conn=10)
```

---

## Additional Resources

- [psycopg2 Connection Documentation](https://www.psycopg.org/docs/connection.html)
- [PostgreSQL Connection Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-KEEPALIVES)
- [psycopg2 Pool Documentation](https://www.psycopg.org/docs/pool.html)
- [PostgreSQL Statistics Views](https://www.postgresql.org/docs/current/monitoring-stats.html)
