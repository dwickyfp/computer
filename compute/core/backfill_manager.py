"""
Backfill manager for processing backfill jobs using DuckDB.

Manages backfill job queue and executes historical data replication.
"""

import logging
import threading
import time
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from core.database import get_connection_pool, get_db_connection
from core.filter_sql import build_where_clause_from_filter_sql
from core.models import Source, QueueBackfillData, BackfillStatus
from core.runtime_metrics import observe, set_gauge
from core.security import decrypt_value
from core.timezone import convert_timestamp_to_target_tz, convert_time_to_target_tz
from config.config import get_config

try:
    import duckdb
except ImportError:
    duckdb = None
    logging.warning("DuckDB not installed. Backfill feature will not work.")

import re as _re

logger = logging.getLogger(__name__)


def _validate_identifier(name: str) -> str:
    """
    BUG-4 FIX: Validate that a SQL identifier contains only safe characters
    before embedding it in a DuckDB query string.

    Allows: letters, digits, underscores, dots (for schema.table notation).
    Raises ValueError for anything that could be an injection vector.
    """
    if not name or not _re.match(r"^[A-Za-z_][A-Za-z0-9_.]*$", name):
        raise ValueError(
            f"Unsafe or invalid SQL identifier: {name!r}. "
            "Identifiers must start with a letter or underscore and contain "
            "only letters, digits, underscores, or dots (for schema.table)."
        )
    return name


class BackfillManager:
    """
    Manages backfill job execution using DuckDB.

    Polls queue_backfill_data table for PENDING jobs and processes them
    using DuckDB's PostgreSQL scanner for efficient batch processing.

    Supports resume from checkpoint after compute engine restart.
    """

    # Configuration constants
    STALE_JOB_THRESHOLD_MINUTES = (
        0  # Recover all EXECUTING jobs on startup (0 = immediate)
    )
    MAX_RESUME_ATTEMPTS = 3  # Fail job after 3 resume attempts

    def __init__(self, check_interval: int = 5, batch_size: int = 10000):
        """
        Initialize backfill manager.

        Args:
            check_interval: Seconds between queue checks
            batch_size: Number of rows per batch
        """
        self.check_interval = check_interval
        self.batch_size = batch_size
        self.stop_event = threading.Event()
        self.active_jobs: dict[int, threading.Thread] = {}
        self.active_jobs_lock = threading.Lock()
        self._max_concurrent_jobs = get_config().runtime.backfill_max_concurrent_jobs

        if not duckdb:
            logger.error("DuckDB is not installed. Install with: pip install duckdb")

    def start(self) -> None:
        """Start the backfill manager thread."""

        # Recover stale jobs from previous compute instance
        self._recover_stale_jobs()

        monitor_thread = threading.Thread(target=self._monitor_queue, daemon=True)
        monitor_thread.start()

    def stop(self) -> None:
        """Stop the backfill manager."""
        self.stop_event.set()

        # BUG-1 FIX: Collect snapshot of threads while holding the lock, then
        # join them OUTSIDE the lock.  Previously the lock was held during
        # thread.join(), which deadlocked because each worker thread's
        # finally-block must also acquire active_jobs_lock to remove itself.
        with self.active_jobs_lock:
            threads = list(self.active_jobs.values())
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=30)

    def _recover_stale_jobs(self) -> None:
        """
        Recover stale EXECUTING jobs from previous compute instance.

        Detects jobs that are stuck in EXECUTING state (likely due to restart)
        and resets them to PENDING for retry, respecting MAX_RESUME_ATTEMPTS.
        """
        conn = None

        try:
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Find stale jobs
                # When threshold is 0, recover ALL EXECUTING jobs on startup
                # Otherwise, recover jobs older than the threshold
                if self.STALE_JOB_THRESHOLD_MINUTES == 0:
                    cursor.execute(
                        """
                    SELECT id, pipeline_id, count_record, total_record, resume_attempts, last_pk_value
                    FROM queue_backfill_data
                    WHERE status = 'EXECUTING'
                    ORDER BY created_at ASC
                """
                    )
                else:
                    cursor.execute(
                        """
                    SELECT id, pipeline_id, count_record, total_record, resume_attempts, last_pk_value
                    FROM queue_backfill_data
                    WHERE status = 'EXECUTING'
                        AND updated_at < NOW() - INTERVAL '%s minutes'
                    ORDER BY created_at ASC
                """,
                        (self.STALE_JOB_THRESHOLD_MINUTES,),
                    )

                stale_jobs = cursor.fetchall()

                if not stale_jobs:
                    return

                for job in stale_jobs:
                    (
                        job_id,
                        pipeline_id,
                        count_record,
                        total_record,
                        resume_attempts,
                        last_pk_value,
                    ) = job
                    progress_pct = (
                        (count_record / total_record * 100) if total_record > 0 else 0
                    )

                    # Check if max resume attempts exceeded
                    if resume_attempts >= self.MAX_RESUME_ATTEMPTS:
                        logger.warning(
                            f"Backfill job {job_id} (pipeline {pipeline_id}) exceeded "
                            f"max resume attempts ({self.MAX_RESUME_ATTEMPTS}). Marking as FAILED."
                        )
                        cursor.execute(
                            """
                        UPDATE queue_backfill_data
                        SET status = 'FAILED',
                            error_message = 'Maximum resume attempts exceeded after compute restart',
                            updated_at = NOW()
                        WHERE id = %s
                    """,
                            (job_id,),
                        )
                    else:
                        # Reset to PENDING for retry — last_pk_value is preserved
                        # so keyset pagination resumes from the exact cursor position
                        resume_info = (
                            f"last_pk_value={last_pk_value}"
                            if last_pk_value
                            else f"count_record={count_record}"
                        )
                        logger.info(
                            f"Recovering backfill job {job_id} (pipeline {pipeline_id}): "
                            f"{progress_pct:.1f}% complete, will resume from {resume_info}"
                        )
                        cursor.execute(
                            """
                        UPDATE queue_backfill_data
                        SET status = 'PENDING',
                            updated_at = NOW()
                        WHERE id = %s
                    """,
                            (job_id,),
                        )

                conn.commit()

        except Exception as e:
            logger.error(f"Error recovering stale jobs: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            # Return connection to pool
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _monitor_queue(self) -> None:
        """Monitor queue for pending backfill jobs."""
        # BUG-20 FIX: Track error sleep separately so repeated DB failures
        # back off exponentially instead of hammering the pool at a fixed rate.
        _error_sleep = self.check_interval
        _MAX_ERROR_SLEEP = 300  # 5-minute ceiling

        while not self.stop_event.is_set():
            try:
                pending_jobs = self._get_pending_jobs()
                _error_sleep = self.check_interval  # reset backoff on success

                for job in pending_jobs:
                    # Check if we should stop
                    if self.stop_event.is_set():
                        break

                    # BUG-6 FIX: Combine "already running?" check AND thread
                    # registration into one atomic lock acquire so two loop
                    # iterations (or two monitor threads) can never start
                    # duplicate threads for the same job.
                    with self.active_jobs_lock:
                        if job["id"] in self.active_jobs:
                            continue
                        job_thread = threading.Thread(
                            target=self._execute_backfill_job,
                            args=(job,),
                            daemon=True,
                        )
                        self.active_jobs[job["id"]] = job_thread

                    job_thread.start()

            except Exception as e:
                logger.error(f"Error in backfill queue monitor: {e}")
                # BUG-20 FIX: Exponential backoff on repeated errors to avoid
                # hammering the connection pool during a DB outage.
                time.sleep(_error_sleep)
                _error_sleep = min(_error_sleep * 2, _MAX_ERROR_SLEEP)
                continue

            # Sleep before next check
            time.sleep(self.check_interval)

    def _get_pending_jobs(self) -> List[dict]:
        """
        Get pending backfill jobs from database.

        Returns:
            List of pending job records
        """
        conn = None

        try:
            with self.active_jobs_lock:
                capacity = max(self._max_concurrent_jobs - len(self.active_jobs), 0)
            set_gauge("backfill.active_jobs", len(self.active_jobs), unit="jobs")
            if capacity <= 0:
                return []

            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # BUG-9 FIX: Use FOR UPDATE OF qb SKIP LOCKED so that multiple
                # compute instances (or two polling cycles) never pick the same
                # row simultaneously.  Immediately UPDATE status to EXECUTING
                # in the same transaction so the rows are not visible to peers
                # once we commit.
                cursor.execute(
                    """
                    SELECT qb.*, s.type AS source_type, s.pg_host, s.pg_port, s.pg_database,
                           s.pg_username, s.pg_password
                    FROM queue_backfill_data qb
                    JOIN sources s ON qb.source_id = s.id
                    WHERE qb.status = %s
                    ORDER BY qb.created_at ASC
                    LIMIT %s
                    FOR UPDATE OF qb SKIP LOCKED
                    """,
                    (BackfillStatus.PENDING.value, capacity),
                )
                jobs = cursor.fetchall()
                result = [dict(job) for job in jobs]

                # Claim all selected rows atomically before releasing the lock
                if result:
                    job_ids = [j["id"] for j in result]
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET status = 'EXECUTING',
                            resume_attempts = COALESCE(resume_attempts, 0) + 1,
                            updated_at = NOW()
                        WHERE id = ANY(%s)
                        """,
                        (job_ids,),
                    )

                conn.commit()
                return result

        except psycopg2.OperationalError as e:
            # Network/server error - connection was closed by server
            logger.error(f"Database connection error fetching pending jobs: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching pending jobs: {e}")
            return []
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _execute_backfill_job(self, job: dict) -> None:
        """
        Execute a backfill job using DuckDB.

        Args:
            job: Job configuration dictionary
        """
        job_id = job["id"]

        try:
            # BUG-9 FIX: _get_pending_jobs() already set status=EXECUTING and
            # incremented resume_attempts atomically in the DB.  Only confirm
            # status here without re-incrementing to avoid double-counting.
            self._update_job_status(
                job_id, BackfillStatus.EXECUTING.value, increment_resume_attempts=False
            )

            # Check if DuckDB is available
            if not duckdb:
                raise RuntimeError("DuckDB is not installed")

            # Execute backfill
            total_records = self._process_backfill_with_duckdb(job)

            # Check if job was cancelled during processing
            if self._is_job_cancelled(job_id):
                # Status is already CANCELLED, just return
                return

            # Update to COMPLETED only if not cancelled
            self._update_job_status(
                job_id,
                BackfillStatus.COMPLETED.value,
                count_record=total_records,
            )

        except Exception as e:
            logger.error(f"Backfill job {job_id} failed: {e}")
            error_msg = str(e)[:500]  # Truncate long error messages
            self._update_job_status(
                job_id,
                BackfillStatus.FAILED.value,
                error_message=error_msg,
            )
        finally:
            # Remove from active jobs
            with self.active_jobs_lock:
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]

    def _process_backfill_with_duckdb(self, job: dict) -> int:
        """
        Process backfill using DuckDB PostgreSQL scanner.

        Uses keyset pagination (WHERE pk > last_pk_value ORDER BY pk LIMIT N)
        instead of LIMIT/OFFSET for consistent O(1) batch fetching regardless
        of progress depth. Persists last_pk_value for crash-safe resume.

        Args:
            job: Job configuration

        Returns:
            Total number of records processed
        """
        job_id = job["id"]
        table_name = job["table_name"]
        filter_sql = job.get("filter_sql")

        if str(job.get("source_type", "POSTGRES")).upper() != "POSTGRES":
            raise ValueError("Backfill is only supported for POSTGRES sources")

        # Get checkpoint for resume
        start_count = job.get("count_record", 0) or 0
        last_pk_value = job.get(
            "last_pk_value"
        )  # Cursor position for keyset pagination
        pk_columns = self._parse_pk_columns(job.get("pk_column"))
        pk_column = pk_columns[0] if len(pk_columns) == 1 else None

        # Build PostgreSQL connection string
        pg_conn_str = self._build_postgres_connection(job)

        # Initialize DuckDB connection (in-memory)
        runtime_cfg = get_config().runtime
        duckdb_mem = runtime_cfg.duckdb_memory_limit
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET memory_limit='{duckdb_mem}'")
        conn.execute(f"SET threads={runtime_cfg.duckdb_threads}")
        conn.execute("SET enable_progress_bar=false")

        total_processed = start_count  # Start from checkpoint

        # Pre-create destination instances once for the entire job
        destinations_cache = self._create_destinations_for_job(job)

        try:
            # Install and load postgres extension
            conn.execute("INSTALL postgres")
            conn.execute("LOAD postgres")

            # BUG-7 FIX: Wrap the ATTACH call and suppress the raw connection
            # string from any exception message so plaintext credentials are
            # never forwarded to the log.
            try:
                conn.execute(f"ATTACH '{pg_conn_str}' AS source_db (TYPE POSTGRES)")
            except Exception as attach_err:
                raise RuntimeError(
                    f"Failed to attach source database "
                    f"[{job.get('pg_host')}:{job.get('pg_port')}"
                    f"/{job.get('pg_database')}]: "
                    f"check host reachability and credentials"
                ) from None  # suppress original which may contain the password

            # Detect primary key columns if not already cached.
            if not pk_columns:
                pk_columns = self._detect_primary_key_columns(conn, table_name)
                if pk_columns:
                    self._update_job_pk_column(job_id, ";".join(pk_columns))
                job["pk_columns"] = pk_columns
                pk_column = pk_columns[0] if len(pk_columns) == 1 else None
            else:
                job["pk_columns"] = pk_columns

            # Build base WHERE clause from filters
            base_where = ""
            if filter_sql:
                where_clause = self._build_backfill_where_clause(filter_sql)
                if where_clause:
                    base_where = where_clause

            # BUG-4 FIX: Validate table_name as a safe SQL identifier before
            # embedding it in query strings to prevent SQL injection.
            safe_table = _validate_identifier(table_name)

            # Build SELECT query for counting (without keyset filter)
            base_query = f"SELECT * FROM source_db.{safe_table}"
            if base_where:
                base_query += f" WHERE {base_where}"

            # Count total rows first
            count_query = f"SELECT COUNT(1) as total FROM ({base_query}) t"
            total_rows = conn.execute(count_query).fetchone()[0]

            # Update total_record in database if not already set
            if job.get("total_record") is None or job.get("total_record") == 0:
                self._update_job_total_record(job_id, total_rows)

            # Determine if we can use keyset pagination
            use_keyset = pk_column is not None

            if use_keyset:
                logger.info(
                    f"Job {job_id}: Using keyset pagination on column '{pk_column}' "
                    f"(resume from last_pk_value={last_pk_value})"
                )
            else:
                logger.info(
                    f"Job {job_id}: No primary key detected, falling back to LIMIT/OFFSET"
                )

            # Process in batches
            offset = start_count  # Only used for OFFSET fallback
            while not self.stop_event.is_set():
                # Check if job was cancelled
                if self._is_job_cancelled(job_id):
                    break

                if use_keyset:
                    # BUG-4 FIX: Validate pk_column as a safe identifier and use
                    # DuckDB positional parameter ($1) for the PK value instead
                    # of naive string interpolation to prevent injection.
                    safe_pk_col = _validate_identifier(pk_column)
                    conditions = []
                    query_params: list = []
                    if base_where:
                        conditions.append(base_where)
                    if last_pk_value is not None:
                        conditions.append(f"{safe_pk_col} > $1")
                        # Preserve original type for DuckDB binding
                        try:
                            query_params.append(int(last_pk_value))
                        except (ValueError, TypeError):
                            try:
                                query_params.append(float(last_pk_value))
                            except (ValueError, TypeError):
                                query_params.append(last_pk_value)

                    where_part = (
                        f" WHERE {' AND '.join(conditions)}" if conditions else ""
                    )

                    batch_query = (
                        f"SELECT * FROM source_db.{safe_table}"
                        f"{where_part}"
                        f" ORDER BY {safe_pk_col} ASC"
                        f" LIMIT {self.batch_size}"
                    )
                else:
                    # Fallback: LIMIT/OFFSET (for tables without PK)
                    query_params = []
                    remaining = total_rows - offset
                    if remaining <= 0:
                        break
                    current_batch_size = min(self.batch_size, remaining)
                    batch_query = (
                        f"{base_query} LIMIT {current_batch_size} OFFSET {offset}"
                    )

                logger.debug(
                    f"Job {job_id}: Processing batch, total_processed={total_processed}"
                )
                fetch_started = time.perf_counter()
                result = conn.execute(batch_query, query_params).fetchall()
                observe(
                    "backfill.fetch_duration",
                    (time.perf_counter() - fetch_started) * 1000.0,
                    unit="ms",
                    job_id=str(job_id),
                    table_name=table_name,
                )

                if not result:
                    break

                # Get column names
                columns = [desc[0] for desc in conn.description]

                # Process batch - convert to CDC events and send to destinations
                batch_records = [dict(zip(columns, row)) for row in result]
                write_started = time.perf_counter()
                self._process_batch_to_destinations(
                    job, batch_records, destinations_cache
                )
                observe(
                    "backfill.destination_write_duration",
                    (time.perf_counter() - write_started) * 1000.0,
                    unit="ms",
                    job_id=str(job_id),
                    table_name=table_name,
                )

                # Update progress and cursor position
                total_processed += len(batch_records)

                if use_keyset:
                    # Track the last PK value for cursor-based resume
                    pk_idx = columns.index(pk_column)
                    last_pk_value = str(result[-1][pk_idx])
                    self._update_job_progress(job_id, total_processed, last_pk_value)
                else:
                    offset += len(batch_records)
                    self._update_job_count(job_id, total_processed)

            return total_processed

        finally:
            conn.close()
            # Close cached destination instances
            self._close_destinations_cache(destinations_cache)

    def _parse_pk_columns(self, pk_value: Optional[str]) -> list[str]:
        if not pk_value:
            return []
        return [column.strip() for column in str(pk_value).split(";") if column.strip()]

    def _detect_primary_key_columns(self, conn, table_name: str) -> list[str]:
        """
        Detect the primary key columns of a table via DuckDB's postgres attachment.

        Returns all PK columns in ordinal order. Backfill pagination still uses
        keyset only for a single-column PK, but row version tracking should use
        the full key set when it exists.

        Args:
            conn: DuckDB connection with source_db attached
            table_name: Table name (may include schema)

        Returns:
            Primary key column names
        """
        try:
            # Parse schema and table from table_name
            if "." in table_name:
                schema, tbl = table_name.rsplit(".", 1)
            else:
                schema = "public"
                tbl = table_name

            # Query PostgreSQL information_schema via DuckDB attachment.
            # BUG-3 FIX: Use DuckDB positional parameters ($1/$2) instead of
            # f-string interpolation to prevent SQL injection via table names.
            result = conn.execute(
                """
                SELECT kcu.column_name
                FROM source_db.information_schema.table_constraints tc
                JOIN source_db.information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = $1
                    AND tc.table_name = $2
                ORDER BY kcu.ordinal_position
                """,
                [schema, tbl],
            ).fetchall()

            pk_columns = [row[0] for row in result]
            if len(pk_columns) == 1:
                logger.info(
                    f"Detected primary key column '{pk_columns[0]}' for table {table_name}"
                )
            elif len(pk_columns) > 1:
                logger.info(
                    f"Composite primary key detected for {table_name} ({pk_columns}), "
                    f"falling back to OFFSET pagination"
                )
            else:
                logger.info(f"No primary key found for {table_name}, using OFFSET")
            return pk_columns

        except Exception as e:
            logger.warning(f"Could not detect PK for {table_name}: {e}")
            return []

    def _update_job_pk_column(self, job_id: int, pk_column: str) -> None:
        """
        Persist the detected PK column name in the job record.

        Args:
            job_id: Job ID
            pk_column: Primary key column name
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET pk_column = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (pk_column, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job pk_column: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _update_job_progress(self, job_id: int, count: int, last_pk_value: str) -> None:
        """
        Update job progress with both record count and cursor position.

        This ensures crash-safe resume: on restart, the job picks up from
        last_pk_value instead of re-scanning via OFFSET.

        Args:
            job_id: Job ID
            count: Current processed record count
            last_pk_value: Last primary key value processed (for keyset resume)
        """
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET count_record = %s, last_pk_value = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (count, last_pk_value, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job progress: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _create_destinations_for_job(self, job: dict) -> dict:
        """
        Create and initialize destination instances once for the entire backfill job.

        Returns a dict mapping (pipeline_destination_id) -> (destination_instance, table_sync, pd_info)
        so batches can reuse them without re-creating connections each time.

        Args:
            job: Job configuration

        Returns:
            Dict mapping pd.id -> {"destination": dest, "table_sync": ts, "pd": pd}
        """
        from core.repository import (
            PipelineRepository,
            DestinationRepository,
            SourceRepository,
        )
        from core.models import DestinationType
        from destinations.snowflake import SnowflakeDestination
        from destinations.postgresql import PostgreSQLDestination
        from destinations.kafka import KafkaDestination

        cache = {}
        try:
            pipeline_id = job["pipeline_id"]
            table_name = job["table_name"]
            source_id = job["source_id"]

            pipeline = PipelineRepository.get_by_id(pipeline_id, include_relations=True)
            if not pipeline or not pipeline.destinations:
                logger.warning(f"Pipeline {pipeline_id} has no destinations configured")
                return cache

            source_config = SourceRepository.get_by_id(source_id)

            for pd in pipeline.destinations:
                table_sync = next(
                    (ts for ts in pd.table_syncs if ts.table_name == table_name),
                    None,
                )
                if not table_sync:
                    continue

                try:
                    destination_config = DestinationRepository.get_by_id(
                        pd.destination_id
                    )
                    if not destination_config:
                        logger.warning(f"Destination {pd.destination_id} not found")
                        continue

                    if (
                        destination_config.type.upper()
                        == DestinationType.SNOWFLAKE.value
                    ):
                        cfg = get_config()
                        timeout_config = {
                            "connect_timeout": cfg.snowflake.connect_timeout,
                            "read_timeout": cfg.snowflake.read_timeout,
                            "write_timeout": cfg.snowflake.write_timeout,
                            "pool_timeout": cfg.snowflake.pool_timeout,
                            "batch_timeout_base": cfg.snowflake.batch_timeout_base,
                            "batch_timeout_max": cfg.snowflake.batch_timeout_max,
                        }
                        dest = SnowflakeDestination(
                            destination_config, timeout_config=timeout_config
                        )
                    elif (
                        destination_config.type.upper()
                        == DestinationType.POSTGRES.value
                    ):
                        dest = PostgreSQLDestination(
                            destination_config, source_config=source_config
                        )
                    elif destination_config.type.upper() == DestinationType.KAFKA.value:
                        dest = KafkaDestination(destination_config)
                    else:
                        logger.warning(
                            f"Unsupported destination type: {destination_config.type}"
                        )
                        continue

                    dest.initialize()
                    cache[pd.id] = {
                        "destination": dest,
                        "table_sync": table_sync,
                        "pd": pd,
                        "pipeline_id": pipeline_id,
                        "source_id": source_id,
                    }
                    logger.info(
                        f"Cached destination {destination_config.name} for backfill job {job['id']}"
                    )

                except Exception as dest_error:
                    logger.error(
                        f"Failed to create destination {pd.destination_id}: {dest_error}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(f"Error creating destinations cache: {e}", exc_info=True)

        return cache

    def _close_destinations_cache(self, cache: dict) -> None:
        """
        Close all cached destination instances.

        Args:
            cache: Destinations cache dict
        """
        for pd_id, entry in cache.items():
            try:
                entry["destination"].close()
            except Exception as e:
                logger.warning(f"Error closing cached destination {pd_id}: {e}")

    def _build_postgres_connection(self, job: dict) -> str:
        """
        Build PostgreSQL connection URI for DuckDB.

        BUG-14 FIX: Use RFC-3986 URI format with URL-encoded credentials instead
        of the libpq keyword-value format.  The keyword format splits on spaces
        and '=' so passwords containing those characters produce malformed DSNs.

        Args:
            job: Job configuration with database details

        Returns:
            URI connection string with URL-encoded credentials
        """
        from urllib.parse import quote_plus

        # Decrypt password if encrypted
        password = decrypt_value(job["pg_password"] or "")
        user = quote_plus(str(job["pg_username"] or ""))
        pw = quote_plus(password)

        return (
            f"postgresql://{user}:{pw}"
            f"@{job['pg_host']}:{job['pg_port']}/{job['pg_database']}"
        )

    def _process_batch_to_destinations(
        self, job: dict, records: List[dict], destinations_cache: Optional[dict] = None
    ) -> None:
        """
        Process batch of records to destinations.

        Uses pre-created destination instances from cache when available,
        falling back to creating new instances per batch if no cache provided.

        Args:
            job: Job configuration
            records: Batch of records to process
            destinations_cache: Optional pre-created destinations cache
        """
        from core.repository import (
            PipelineRepository,
            DestinationRepository,
            SourceRepository,
            DataFlowRepository,
        )
        from core.models import DestinationType
        from destinations.base import CDCRecord
        from destinations.snowflake import SnowflakeDestination
        from destinations.postgresql import PostgreSQLDestination
        from destinations.kafka import KafkaDestination
        from decimal import Decimal
        from datetime import date, datetime
        import json

        try:
            pipeline_id = job["pipeline_id"]
            table_name = job["table_name"]
            source_id = job["source_id"]

            logger.debug(
                f"Processing {len(records)} records to destinations for pipeline {pipeline_id}"
            )

            # Convert records to CDC format with proper serialization
            cdc_records = []
            key_columns = job.get("pk_columns") or self._parse_pk_columns(job.get("pk_column"))
            # BUG-12 FIX: Use current wall-clock timestamp instead of None so
            # the DLQ version tracker can compare backfill records against
            # live CDC records and avoid re-applying stale replays.
            import time as _time

            _batch_ts = int(_time.time() * 1000)
            for record in records:
                # Serialize problematic types (Decimal, datetime, etc.)
                serialized_record = self._serialize_record(record)

                cdc_record = CDCRecord(
                    operation="r",  # 'r' = read/snapshot operation
                    table_name=table_name,
                    key=self._extract_keys(serialized_record, key_columns),
                    value=serialized_record,
                    schema=None,
                    timestamp=_batch_ts,
                )
                cdc_records.append(cdc_record)

            # Use cached destinations if available (Bottleneck 7 optimization)
            if destinations_cache:
                # BUG-18 FIX: Track write failures so the job is marked FAILED
                # rather than silently marked COMPLETED with missing data.
                _write_failure = False
                for pd_id, entry in destinations_cache.items():
                    try:
                        dest = entry["destination"]
                        table_sync = entry["table_sync"]
                        pd_info = entry["pd"]

                        # Ensure destination is still initialized
                        if not dest._is_initialized:
                            dest.initialize()

                        written = dest.write_batch(cdc_records, table_sync)

                        if written > 0:
                            monitoring_table_name = (
                                f"LANDING_{table_name.upper()}"
                                if dest._config.type.upper()
                                == DestinationType.SNOWFLAKE.value
                                else table_sync.table_name_target
                            )
                            try:
                                DataFlowRepository.increment_count(
                                    pipeline_id=pipeline_id,
                                    pipeline_destination_id=pd_id,
                                    source_id=source_id,
                                    table_sync_id=table_sync.id,
                                    table_name=monitoring_table_name,
                                    count=written,
                                )
                            except Exception as monitoring_error:
                                logger.warning(
                                    f"Failed to update data flow monitoring: {monitoring_error}"
                                )

                    except Exception as dest_error:
                        logger.error(
                            f"Failed to write batch to destination {pd_id}: {dest_error}",
                            exc_info=True,
                        )
                        _write_failure = True

                if _write_failure:
                    raise RuntimeError(
                        f"One or more destination writes failed for backfill job "
                        f"{job.get('id', '?')} — batch will not be marked complete."
                    )
                return

            # Fallback: create destinations per batch (legacy path)
            pipeline = PipelineRepository.get_by_id(pipeline_id, include_relations=True)

            if not pipeline or not pipeline.destinations:
                logger.warning(f"Pipeline {pipeline_id} has no destinations configured")
                return

            # Get source config for PostgreSQL destination joins
            source_config = SourceRepository.get_by_id(source_id)

            # Write batch to each destination
            for pd in pipeline.destinations:
                # Find matching table sync
                table_sync = next(
                    (ts for ts in pd.table_syncs if ts.table_name == table_name),
                    None,
                )

                if not table_sync:
                    logger.debug(
                        f"No table sync for {table_name} in destination {pd.destination_id}"
                    )
                    continue

                destination = None
                try:
                    # Get destination config
                    destination_config = DestinationRepository.get_by_id(
                        pd.destination_id
                    )
                    if not destination_config:
                        logger.warning(f"Destination {pd.destination_id} not found")
                        continue

                    # Create destination instance
                    if (
                        destination_config.type.upper()
                        == DestinationType.SNOWFLAKE.value
                    ):
                        # Get Snowflake timeout config from global config
                        cfg = get_config()
                        timeout_config = {
                            "connect_timeout": cfg.snowflake.connect_timeout,
                            "read_timeout": cfg.snowflake.read_timeout,
                            "write_timeout": cfg.snowflake.write_timeout,
                            "pool_timeout": cfg.snowflake.pool_timeout,
                            "batch_timeout_base": cfg.snowflake.batch_timeout_base,
                            "batch_timeout_max": cfg.snowflake.batch_timeout_max,
                        }
                        destination = SnowflakeDestination(
                            destination_config, timeout_config=timeout_config
                        )
                    elif (
                        destination_config.type.upper()
                        == DestinationType.POSTGRES.value
                    ):
                        destination = PostgreSQLDestination(
                            destination_config, source_config=source_config
                        )
                    elif destination_config.type.upper() == DestinationType.KAFKA.value:
                        destination = KafkaDestination(destination_config)
                    else:
                        logger.warning(
                            f"Unsupported destination type: {destination_config.type}"
                        )
                        continue

                    # Initialize destination
                    destination.initialize()

                    # Write batch to destination
                    written = destination.write_batch(cdc_records, table_sync)

                    # Track data flow monitoring (same as CDC)
                    if written > 0:
                        monitoring_table_name = (
                            f"LANDING_{table_name.upper()}"
                            if destination_config.type.upper()
                            == DestinationType.SNOWFLAKE.value
                            else table_sync.table_name_target
                        )
                        try:
                            DataFlowRepository.increment_count(
                                pipeline_id=pipeline_id,
                                pipeline_destination_id=pd.id,
                                source_id=job["source_id"],
                                table_sync_id=table_sync.id,
                                table_name=monitoring_table_name,
                                count=written,
                            )
                        except Exception as monitoring_error:
                            logger.warning(
                                f"Failed to update data flow monitoring: {monitoring_error}"
                            )

                except Exception as dest_error:
                    logger.error(
                        f"Failed to write batch to destination {pd.destination_id}: {dest_error}",
                        exc_info=True,
                    )
                    # BUG-18 FIX: Re-raise so _execute_backfill_job marks the
                    # job FAILED instead of silently marking it COMPLETED while
                    # data is missing from this destination.
                    raise
                finally:
                    # Always close the destination to release connections/resources
                    if destination is not None:
                        try:
                            destination.close()
                        except Exception as close_error:
                            logger.warning(
                                f"Error closing destination {pd.destination_id}: {close_error}"
                            )

        except Exception as e:
            logger.error(f"Error processing batch to destinations: {e}", exc_info=True)
            raise

    def _extract_keys(self, record: dict, key_columns: Optional[list[str]] = None) -> dict:
        """
        Extract primary key fields from record.

        Args:
            record: Record dictionary
            key_columns: Detected primary key columns in ordinal order

        Returns:
            Dictionary with key fields
        """
        selected_columns = key_columns or (["id"] if "id" in record else [])
        return {
            column: record[column]
            for column in selected_columns
            if column in record and record[column] is not None
        }

    def _serialize_record(self, record: dict) -> dict:
        """
        Serialize record values to handle problematic types.

        Converts Decimal, datetime, date, and other non-JSON-serializable types
        to formats that Snowflake destinations can handle properly.

        DuckDB PostgreSQL Scanner Type Mapping:
        - PostgreSQL TIMESTAMP → Python datetime (naive, no tzinfo)
        - PostgreSQL TIMESTAMPTZ → Python datetime (aware, tzinfo preserved)
        - PostgreSQL NUMERIC/DECIMAL → Python Decimal
        - PostgreSQL DATE → Python date
        - PostgreSQL TIME → Python time
        - PostgreSQL UUID → Python UUID
        - PostgreSQL BYTEA → Python bytes

        Snowflake Target Type Mapping:
        - TIMESTAMP_NTZ ← datetime without timezone suffix
        - TIMESTAMP_TZ ← datetime with timezone suffix (preserves original TZ)
        - NUMBER/NUMERIC ← string (preserves precision)

        Args:
            record: Raw record dictionary from DuckDB

        Returns:
            Serialized record dictionary
        """
        from decimal import Decimal
        from datetime import date, datetime, time
        from uuid import UUID

        serialized = {}
        for key, value in record.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, Decimal):
                # Convert Decimal to string to preserve precision for Snowflake NUMERIC
                # DO NOT use float() as it loses precision for high-precision decimals
                serialized[key] = str(value)
            elif isinstance(value, datetime):
                # DuckDB returns:
                # - TIMESTAMP (without TZ) → naive datetime (no tzinfo)
                # - TIMESTAMPTZ (with TZ) → aware datetime (tzinfo preserved)
                #
                # Snowflake expects:
                # - TIMESTAMP_NTZ ← "2024-01-15T10:30:00.000000" (no TZ suffix)
                # - TIMESTAMP_TZ ← "2024-01-15T10:30:00.000000+07:00" (converted to target TZ)
                if value.tzinfo is not None:
                    # Has timezone info → TIMESTAMPTZ → TIMESTAMP_TZ
                    # Convert to target timezone (Asia/Jakarta) for consistency
                    converted = convert_timestamp_to_target_tz(value)
                    serialized[key] = converted.isoformat()
                else:
                    # No timezone info → TIMESTAMP → TIMESTAMP_NTZ
                    # Output without timezone for Snowflake TIMESTAMP_NTZ
                    serialized[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%f")
            elif isinstance(value, date):
                # DATE → ISO format string "YYYY-MM-DD"
                serialized[key] = value.isoformat()
            elif isinstance(value, time):
                # TIME → ISO format string with or without TZ
                if value.tzinfo is not None:
                    # TIME WITH TIME ZONE → convert to target timezone offset
                    # Output format: "HH:MM:SS.ffffff+HH:MM" (ISO-8601 with offset)
                    # PostgreSQL can parse this format directly
                    converted = convert_time_to_target_tz(value)
                    serialized[key] = converted.isoformat()
                else:
                    # TIME WITHOUT TIME ZONE → no offset
                    serialized[key] = value.strftime("%H:%M:%S.%f")
            elif isinstance(value, UUID):
                # UUID → string
                serialized[key] = str(value)
            elif isinstance(value, (bytes, bytearray)):
                # BYTEA/geometry WKB → hex string
                serialized[key] = value.hex()
            elif isinstance(value, dict):
                # JSON/JSONB → keep as dict for VARIANT
                serialized[key] = value
            elif isinstance(value, list):
                # Array types → keep as list
                serialized[key] = value
            elif isinstance(value, bool):
                # Boolean → keep as-is
                serialized[key] = value
            elif isinstance(value, (int, float)):
                # Numeric primitives → keep as-is
                serialized[key] = value
            elif isinstance(value, str):
                # String → keep as-is
                serialized[key] = value
            else:
                # Unknown types → convert to string
                logger.warning(
                    f"Unknown type {type(value).__name__} for column {key}, converting to string"
                )
                serialized[key] = str(value)

        return serialized

    def _update_job_status(
        self,
        job_id: int,
        status: str,
        count_record: Optional[int] = None,
        error_message: Optional[str] = None,
        increment_resume_attempts: bool = False,
    ) -> None:
        """
        Update backfill job status in database.

        Args:
            job_id: Job ID
            status: New status
            count_record: Optional record count
            error_message: Optional error message for failed jobs
            increment_resume_attempts: Whether to increment resume_attempts counter
        """
        conn = None

        # Set is_error flag if status is FAILED
        is_error = status == BackfillStatus.FAILED.value

        try:
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()
            with conn.cursor() as cursor:
                # Build SQL query dynamically based on parameters
                update_fields = ["status = %s", "is_error = %s", "updated_at = NOW()"]
                params = [status, is_error]

                if count_record is not None:
                    update_fields.append("count_record = %s")
                    params.append(count_record)

                if error_message is not None:
                    update_fields.append("error_message = %s")
                    params.append(error_message)

                if increment_resume_attempts:
                    update_fields.append(
                        "resume_attempts = COALESCE(resume_attempts, 0) + 1"
                    )

                params.append(job_id)

                query = f"""
                    UPDATE queue_backfill_data
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                """

                cursor.execute(query, params)
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job status: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _update_job_count(self, job_id: int, count: int) -> None:
        """
        Update job record count.

        Args:
            job_id: Job ID
            count: Current count
        """
        conn = None

        try:
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET count_record = %s
                    WHERE id = %s
                    """,
                    (count, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job count: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _build_backfill_where_clause(self, filter_sql: str) -> str:
        """
        Build WHERE clause from filter_sql for backfill queries.

        Args:
            filter_sql: Filter SQL string in JSON v2 format

        Returns:
            WHERE clause string (without WHERE keyword), or empty string
        """
        return build_where_clause_from_filter_sql(filter_sql, error_cls=ValueError)

    def _build_where_clause_v2(self, parsed: dict) -> str:
        """
        Build WHERE clause from JSON v2 filter format.

        Args:
            parsed: Parsed JSON v2 filter dict

        Returns:
            WHERE clause string
        """
        groups = parsed.get("groups", [])
        inter_logic = parsed.get("interLogic", [])

        if not groups:
            return ""

        group_clauses = []
        for group in groups:
            conditions = group.get("conditions", [])
            intra_logic = group.get("intraLogic", "AND")

            if not conditions:
                continue

            clauses = []
            for cond in conditions:
                column = cond.get("column", "")
                operator = cond.get("operator", "")
                value = cond.get("value", "")
                value2 = cond.get("value2", "")

                if not column:
                    continue

                clause = self._build_single_clause(column, operator, value, value2)
                if clause:
                    clauses.append(clause)

            if not clauses:
                continue

            if len(clauses) == 1:
                group_clauses.append(clauses[0])
            else:
                group_clauses.append(f"({f' {intra_logic} '.join(clauses)})")

        if not group_clauses:
            return ""

        result = group_clauses[0]
        for i in range(1, len(group_clauses)):
            logic = inter_logic[i - 1] if i - 1 < len(inter_logic) else "AND"
            result = f"{result} {logic} {group_clauses[i]}"

        return result

    def _build_single_clause(
        self, column: str, operator: str, value: str, value2: str = ""
    ) -> str:
        """
        Build a single SQL clause from filter components.

        Args:
            column: Column name
            operator: SQL operator
            value: Filter value
            value2: Second value (for BETWEEN)

        Returns:
            SQL clause string
        """
        op_upper = operator.upper().strip()

        if op_upper in ("IS NULL", "IS NOT NULL"):
            return f"{column} {op_upper}"

        if not value and op_upper not in ("IS NULL", "IS NOT NULL"):
            return ""

        if op_upper == "BETWEEN" and value and value2:
            q_val = self._quote_value(value)
            q_val2 = self._quote_value(value2)
            return f"{column} BETWEEN {q_val} AND {q_val2}"

        if op_upper in ("LIKE", "ILIKE"):
            return f"{column} {op_upper} '%{value}%'"

        if op_upper == "IN":
            values = [v.strip() for v in value.split(",") if v.strip()]
            quoted = [self._quote_value(v) for v in values]
            return f"{column} IN ({', '.join(quoted)})"

        return f"{column} {operator} {self._quote_value(value)}"

    def _quote_value(self, value: str) -> str:
        """Quote a filter value - numeric values unquoted, strings quoted."""
        try:
            float(value)
            return value
        except (ValueError, TypeError):
            return f"'{value}'"

    def _update_job_total_record(self, job_id: int, total: int) -> None:
        """
        Update job total record count.

        Args:
            job_id: Job ID
            total: Total number of records to process
        """
        conn = None

        try:
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET total_record = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (total, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job total record: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:  # BUG-16 FIX: no bare except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _is_job_cancelled(self, job_id: int) -> bool:
        """
        Check if job was cancelled.

        Args:
            job_id: Job ID

        Returns:
            True if cancelled
        """
        conn = None

        try:
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM queue_backfill_data WHERE id = %s",
                    (job_id,),
                )
                result = cursor.fetchone()
                return result and result[0] == BackfillStatus.CANCELLED.value
        except Exception as e:
            logger.error(f"Error checking job cancellation: {e}")
            return False
        finally:
            if conn:
                from core.database import return_db_connection

                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")
