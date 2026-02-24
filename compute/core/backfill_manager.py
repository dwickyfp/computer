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
from core.models import Source, QueueBackfillData, BackfillStatus
from core.security import decrypt_value
from core.timezone import convert_timestamp_to_target_tz, convert_time_to_target_tz
from config.config import get_config

try:
    import duckdb
except ImportError:
    duckdb = None
    logging.warning("DuckDB not installed. Backfill feature will not work.")

logger = logging.getLogger(__name__)


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

        # Wait for active jobs to complete (with timeout)
        with self.active_jobs_lock:
            for job_id, thread in list(self.active_jobs.items()):
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
                    job_id, pipeline_id, count_record, total_record, resume_attempts, last_pk_value = (
                        job
                    )
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
                        resume_info = f"last_pk_value={last_pk_value}" if last_pk_value else f"count_record={count_record}"
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
                except:
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

        while not self.stop_event.is_set():
            try:
                pending_jobs = self._get_pending_jobs()

                for job in pending_jobs:
                    # Check if we should stop
                    if self.stop_event.is_set():
                        break

                    # Skip if already processing
                    with self.active_jobs_lock:
                        if job["id"] in self.active_jobs:
                            continue

                    # Start job in background thread
                    job_thread = threading.Thread(
                        target=self._execute_backfill_job,
                        args=(job,),
                        daemon=True,
                    )

                    with self.active_jobs_lock:
                        self.active_jobs[job["id"]] = job_thread

                    job_thread.start()

            except Exception as e:
                logger.error(f"Error in backfill queue monitor: {e}")

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
            # get_db_connection() handles retries on pool exhaustion
            conn = get_db_connection()

            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT qb.*, s.pg_host, s.pg_port, s.pg_database, 
                           s.pg_username, s.pg_password
                    FROM queue_backfill_data qb
                    JOIN sources s ON qb.source_id = s.id
                    WHERE qb.status = %s
                    ORDER BY qb.created_at ASC
                    LIMIT 10
                    """,
                    (BackfillStatus.PENDING.value,),
                )
                jobs = cursor.fetchall()
                result = [dict(job) for job in jobs]
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
            # Update status to EXECUTING and increment resume_attempts
            self._update_job_status(
                job_id, BackfillStatus.EXECUTING.value, increment_resume_attempts=True
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

        # Get checkpoint for resume
        start_count = job.get("count_record", 0) or 0
        last_pk_value = job.get("last_pk_value")  # Cursor position for keyset pagination
        pk_column = job.get("pk_column")  # Cached PK column name

        # Build PostgreSQL connection string
        pg_conn_str = self._build_postgres_connection(job)

        # Initialize DuckDB connection (in-memory)
        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit='8GB'")
        conn.execute("SET threads=4")
        conn.execute("SET enable_progress_bar=false")

        total_processed = start_count  # Start from checkpoint

        # Pre-create destination instances once for the entire job
        destinations_cache = self._create_destinations_for_job(job)

        try:
            # Install and load postgres extension
            conn.execute("INSTALL postgres")
            conn.execute("LOAD postgres")

            # Attach PostgreSQL database
            conn.execute(
                f"""
                ATTACH '{pg_conn_str}' AS source_db (TYPE POSTGRES)
                """
            )

            # Detect primary key column if not already cached
            if not pk_column:
                pk_column = self._detect_primary_key(conn, table_name)
                if pk_column:
                    self._update_job_pk_column(job_id, pk_column)

            # Build base WHERE clause from filters
            base_where = ""
            if filter_sql:
                where_clause = self._build_backfill_where_clause(filter_sql)
                if where_clause:
                    base_where = where_clause

            # Build SELECT query for counting (without keyset filter)
            base_query = f"SELECT * FROM source_db.{table_name}"
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
                    # Build keyset pagination query
                    conditions = []
                    if base_where:
                        conditions.append(base_where)
                    if last_pk_value is not None:
                        # Quote string PKs, leave numeric PKs unquoted
                        try:
                            float(last_pk_value)
                            pk_literal = last_pk_value
                        except (ValueError, TypeError):
                            pk_literal = f"'{last_pk_value}'"
                        conditions.append(f"{pk_column} > {pk_literal}")

                    where_part = ""
                    if conditions:
                        where_part = f" WHERE {' AND '.join(conditions)}"

                    batch_query = (
                        f"SELECT * FROM source_db.{table_name}"
                        f"{where_part}"
                        f" ORDER BY {pk_column} ASC"
                        f" LIMIT {self.batch_size}"
                    )
                else:
                    # Fallback: LIMIT/OFFSET (for tables without PK)
                    remaining = total_rows - offset
                    if remaining <= 0:
                        break
                    current_batch_size = min(self.batch_size, remaining)
                    batch_query = f"{base_query} LIMIT {current_batch_size} OFFSET {offset}"

                logger.debug(
                    f"Job {job_id}: Processing batch, total_processed={total_processed}"
                )
                result = conn.execute(batch_query).fetchall()

                if not result:
                    break

                # Get column names
                columns = [desc[0] for desc in conn.description]

                # Process batch - convert to CDC events and send to destinations
                batch_records = [dict(zip(columns, row)) for row in result]
                self._process_batch_to_destinations(
                    job, batch_records, destinations_cache
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

    def _detect_primary_key(self, conn, table_name: str) -> Optional[str]:
        """
        Detect the primary key column of a table via DuckDB's postgres attachment.

        Returns a single PK column name, or None if no PK or composite PK.
        For composite PKs, falls back to OFFSET pagination.

        Args:
            conn: DuckDB connection with source_db attached
            table_name: Table name (may include schema)

        Returns:
            Primary key column name, or None
        """
        try:
            # Parse schema and table from table_name
            if "." in table_name:
                schema, tbl = table_name.rsplit(".", 1)
            else:
                schema = "public"
                tbl = table_name

            # Query PostgreSQL information_schema via DuckDB attachment
            result = conn.execute(
                f"""
                SELECT kcu.column_name
                FROM source_db.information_schema.table_constraints tc
                JOIN source_db.information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{schema}'
                    AND tc.table_name = '{tbl}'
                ORDER BY kcu.ordinal_position
                """
            ).fetchall()

            if len(result) == 1:
                pk_col = result[0][0]
                logger.info(f"Detected primary key column '{pk_col}' for table {table_name}")
                return pk_col
            elif len(result) > 1:
                logger.info(
                    f"Composite primary key detected for {table_name} "
                    f"({[r[0] for r in result]}), falling back to OFFSET"
                )
                return None
            else:
                logger.info(f"No primary key found for {table_name}, using OFFSET")
                return None

        except Exception as e:
            logger.warning(f"Could not detect PK for {table_name}: {e}")
            return None

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
                except:
                    pass
        finally:
            if conn:
                from core.database import return_db_connection
                try:
                    return_db_connection(conn)
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")

    def _update_job_progress(
        self, job_id: int, count: int, last_pk_value: str
    ) -> None:
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
                except:
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
        Build PostgreSQL connection string for DuckDB.

        Args:
            job: Job configuration with database details

        Returns:
            Connection string with decrypted password
        """
        # Decrypt password if encrypted
        password = decrypt_value(job["pg_password"] or "")

        return (
            f"dbname={job['pg_database']} "
            f"user={job['pg_username']} "
            f"password={password} "
            f"host={job['pg_host']} "
            f"port={job['pg_port']}"
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
            for record in records:
                # Serialize problematic types (Decimal, datetime, etc.)
                serialized_record = self._serialize_record(record)

                cdc_record = CDCRecord(
                    operation="r",  # 'r' = read/snapshot operation
                    table_name=table_name,
                    key=self._extract_keys(serialized_record),
                    value=serialized_record,
                    schema=None,
                    timestamp=None,
                )
                cdc_records.append(cdc_record)

            # Use cached destinations if available (Bottleneck 7 optimization)
            if destinations_cache:
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
                            try:
                                DataFlowRepository.increment_count(
                                    pipeline_id=pipeline_id,
                                    pipeline_destination_id=pd_id,
                                    source_id=source_id,
                                    table_sync_id=table_sync.id,
                                    table_name=f"LANDING_{table_name.upper()}",
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
                        try:
                            DataFlowRepository.increment_count(
                                pipeline_id=pipeline_id,
                                pipeline_destination_id=pd.id,
                                source_id=job["source_id"],
                                table_sync_id=table_sync.id,
                                table_name=f"LANDING_{table_name.upper()}",
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
                    # Continue with other destinations even if one fails

        except Exception as e:
            logger.error(f"Error processing batch to destinations: {e}", exc_info=True)
            raise

    def _extract_keys(self, record: dict) -> dict:
        """
        Extract primary key fields from record.

        Args:
            record: Record dictionary

        Returns:
            Dictionary with key fields (simple heuristic - look for 'id' field)
        """
        # Simple heuristic - in production you should query the table schema
        # to get actual primary keys
        if "id" in record:
            return {"id": record["id"]}
        return {}

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
                except:
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
                except:
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

        Supports both legacy semicolon format and JSON v2 format with
        AND/OR grouping and IN operator.

        Args:
            filter_sql: Filter SQL string (legacy or JSON v2)

        Returns:
            WHERE clause string (without WHERE keyword), or empty string
        """
        if not filter_sql:
            return ""

        # Try JSON v2 format first
        try:
            import json
            parsed = json.loads(filter_sql)
            if isinstance(parsed, dict) and parsed.get("version") == 2:
                return self._build_where_clause_v2(parsed)
        except (json.JSONDecodeError, TypeError):
            pass

        # Legacy format: semicolon-separated, all AND
        where_clauses = filter_sql.split(";")
        return " AND ".join(
            f"({clause.strip()})" for clause in where_clauses if clause.strip()
        )

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

    def _build_single_clause(self, column: str, operator: str, value: str, value2: str = "") -> str:
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
                except:
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
