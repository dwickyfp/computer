# Snowflake Timeout Configuration

## Problem

When Snowflake destinations are slow or overloaded, operations can timeout and get enqueued to the Dead Letter Queue (DLQ) with errors like:

```
Operation timed out - destination may be slow or overloaded
```

## Solution

Rosetta now supports configurable timeouts for Snowflake destinations via environment variables.

## Configuration

Add these to your `.env` file:

```bash
# Snowflake Destination Timeouts
SNOWFLAKE_CONNECT_TIMEOUT=30.0       # Connection establishment (seconds)
SNOWFLAKE_READ_TIMEOUT=300.0         # HTTP read timeout (seconds)
SNOWFLAKE_WRITE_TIMEOUT=60.0         # HTTP write timeout (seconds)
SNOWFLAKE_POOL_TIMEOUT=10.0          # Connection pool timeout (seconds)
SNOWFLAKE_BATCH_TIMEOUT_BASE=300     # Base timeout for write_batch (seconds)
SNOWFLAKE_BATCH_TIMEOUT_MAX=600      # Maximum timeout for write_batch (seconds)
```

### Default Values (Previous Hardcoded Values)

- `SNOWFLAKE_READ_TIMEOUT`: **120s → 300s** (2.5x increase)
- `SNOWFLAKE_BATCH_TIMEOUT_BASE`: **120s → 300s** (2.5x increase)
- `SNOWFLAKE_BATCH_TIMEOUT_MAX`: **300s → 600s** (2x increase)

## Timeout Behavior

### HTTP Client Timeouts

Applied to all HTTP requests to Snowflake:

- **Connect**: Time to establish TCP connection
- **Read**: Time to receive response from Snowflake
- **Write**: Time to send request body
- **Pool**: Time to acquire connection from pool

### Write Batch Timeout

Dynamic timeout for batch operations:

```
timeout = min(BATCH_TIMEOUT_BASE + (num_records / 100), BATCH_TIMEOUT_MAX)
```

Example for 5000 records:

```
timeout = min(300 + (5000 / 100), 600)
        = min(300 + 50, 600)
        = 350 seconds
```

## Recommendations

### For Slow Snowflake Instances

If you frequently see timeout errors, increase these values:

```bash
SNOWFLAKE_READ_TIMEOUT=600.0         # 10 minutes
SNOWFLAKE_BATCH_TIMEOUT_BASE=600     # 10 minutes base
SNOWFLAKE_BATCH_TIMEOUT_MAX=1200     # 20 minutes max
```

### For High-Volume Pipelines

Large batches need more time:

```bash
SNOWFLAKE_BATCH_TIMEOUT_BASE=300     # 5 minutes base
SNOWFLAKE_BATCH_TIMEOUT_MAX=1800     # 30 minutes max
```

### For Network Issues

If connection establishment is slow:

```bash
SNOWFLAKE_CONNECT_TIMEOUT=60.0       # 1 minute
SNOWFLAKE_POOL_TIMEOUT=20.0          # 20 seconds
```

## Monitoring

Check DLQ for timeout errors:

```bash
# Redis CLI
redis-cli --scan --pattern "rosetta:dlq:*" | \
  xargs -I {} sh -c 'echo "{}:"; redis-cli XLEN {}'
```

Check compute logs:

```bash
grep "timed out" logs/compute.log
```

## Applying Changes

1. Update `.env` file with desired timeout values
2. Restart compute service:
   ```bash
   # Stop running pipelines
   # Restart compute
   python main.py
   ```

**Note**: Changes require compute service restart. Existing pipelines will use new timeouts after restart.

## Troubleshooting

### Still Getting Timeouts

1. **Check Snowflake performance**: Query `QUERY_HISTORY` view
2. **Check network latency**: Test with `curl` or `ping`
3. **Reduce batch size**: Lower `PIPELINE_MAX_BATCH_SIZE` in config
4. **Scale Snowflake**: Increase warehouse size

### DLQ Growing Too Fast

1. Increase timeouts as shown above
2. Enable DLQ recovery worker (already running by default)
3. Check `DLQ_CHECK_INTERVAL` (default 30s)

### Operations Still Failing After Timeout Increase

This indicates a deeper issue:

- Snowflake warehouse suspended/stopped
- Network connectivity problems
- Invalid credentials
- Table schema mismatches

Check compute logs for specific error messages.
