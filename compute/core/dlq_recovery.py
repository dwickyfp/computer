"""
DLQ Recovery Worker - Background thread for replaying dead letter queue messages.

Continuously monitors DLQ queues and attempts to replay messages when destinations
become available. Uses Redis Streams consumer groups for at-least-once delivery.
"""

import logging
import threading
import time
from typing import Optional

from destinations.base import BaseDestination, CDCRecord
from core.dlq_manager import DLQManager, DLQMessage
from core.models import (
    Pipeline,
    PipelineDestinationTableSync,
)
from core.exceptions import DestinationException
from core.repository import (
    PipelineDestinationRepository,
    TableSyncRepository,
    TableMetadataRepository,
)

logger = logging.getLogger(__name__)


class DLQRecoveryWorker:
    """
    Background worker for recovering messages from DLQ.

    Monitors all DLQ queues for a pipeline and attempts to replay messages
    when destinations are healthy and reachable. Uses Redis Streams consumer
    groups — unacknowledged messages are automatically retried on next cycle.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: DLQManager,
        check_interval: int = 30,
        batch_size: int = 100,
        max_retry_count: int = 10,
        max_age_days: int = 7,
        consumer_name: str = "worker-1",
    ):
        """
        Initialize DLQ recovery worker.

        Args:
            pipeline: Pipeline configuration
            destinations: Dict of destination_id -> BaseDestination
            dlq_manager: DLQ manager instance
            check_interval: Seconds between recovery attempts
            batch_size: Number of messages to process per batch
            max_retry_count: Max retries before discarding a message
            max_age_days: Max age in days before purging a message
            consumer_name: Consumer name within the consumer group
        """
        self._pipeline = pipeline
        self._destinations = destinations
        self._dlq_manager = dlq_manager
        self._check_interval = check_interval
        self._batch_size = batch_size
        self._max_retry_count = max_retry_count
        self._max_age_days = max_age_days
        self._consumer_name = consumer_name

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._logger = logging.getLogger(f"{__name__}.Pipeline_{pipeline.id}")

        # Track destination health: destination_id -> last_check_success
        self._destination_health: dict[int, bool] = {}
        self._health_check_lock = threading.Lock()

        # Track if this is the first iteration (for startup recovery)
        self._first_iteration = True

    def start(self) -> None:
        """Start the recovery worker thread."""
        if self._running:
            self._logger.warning("DLQ recovery worker already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_recovery_loop,
            name=f"DLQRecovery-Pipeline{self._pipeline.id}",
            daemon=True,
        )
        self._thread.start()
        self._logger.info(
            f"Started DLQ recovery worker for pipeline {self._pipeline.name} "
            f"(check_interval={self._check_interval}s, batch_size={self._batch_size})"
        )

    def stop(self) -> None:
        """Stop the recovery worker thread."""
        if not self._running:
            return

        self._logger.info(
            f"Stopping DLQ recovery worker for pipeline {self._pipeline.name}"
        )
        self._running = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                self._logger.warning("DLQ recovery worker did not stop gracefully")

    def _run_recovery_loop(self) -> None:
        """Main recovery loop - runs continuously until stopped."""
        self._logger.info("DLQ recovery loop started")

        # Track iterations for periodic cleanup
        iteration_count = 0
        cleanup_interval = (
            10  # Run purge every 10 iterations (10 * check_interval seconds)
        )

        while self._running:
            try:
                iteration_count += 1

                # Get all queue identifiers from DLQ
                queues = self._dlq_manager.list_queues()

                # On startup: Clean up unused queues FIRST before attempting recovery
                if self._first_iteration and queues:
                    self._logger.info(
                        "STARTUP: Cleaning up unused DLQ queues before recovery"
                    )
                    self._cleanup_orphaned_queues(queues)
                    # Refresh queue list after cleanup
                    queues = self._dlq_manager.list_queues()

                if queues:
                    if self._first_iteration:
                        self._logger.info(
                            f"STARTUP RECOVERY: Processing {len(queues)} DLQ queues "
                            f"for existing messages"
                        )
                    else:
                        self._logger.debug(f"Found {len(queues)} DLQ queues to process")

                    for source_id, table_name, destination_id in queues:
                        if not self._running:
                            break

                        # On startup (first iteration), aggressively claim ALL pending
                        # messages with min_idle=0 to recover messages from previous run
                        if self._first_iteration:
                            self._claim_and_process_stale(
                                source_id, table_name, destination_id, min_idle_ms=0
                            )
                        else:
                            # Normal operation: claim stale messages from dead consumers
                            self._claim_and_process_stale(
                                source_id, table_name, destination_id
                            )

                        # Then process new messages
                        self._process_queue(source_id, table_name, destination_id)

                        # Periodic cleanup: purge old messages
                        if iteration_count % cleanup_interval == 0:
                            self._dlq_manager.purge_old_messages(
                                source_id,
                                table_name,
                                destination_id,
                                max_retry_count=self._max_retry_count,
                                max_age_days=self._max_age_days,
                            )

                # Periodic cleanup: remove unused DLQ queues for removed destinations/tables
                if iteration_count % cleanup_interval == 0:
                    if queues:
                        self._logger.debug(
                            f"Running DLQ cleanup check for {len(queues)} queue(s)"
                        )
                    self._cleanup_orphaned_queues(queues)

                # Mark first iteration complete
                if self._first_iteration:
                    self._first_iteration = False
                    self._logger.info(
                        "STARTUP RECOVERY: Completed first iteration, "
                        "switching to normal recovery mode"
                    )

                # Sleep before next check
                time.sleep(self._check_interval)

            except Exception as e:
                self._logger.error(f"Error in DLQ recovery loop: {e}", exc_info=True)
                time.sleep(self._check_interval)

        self._logger.info("DLQ recovery loop stopped")

    def _claim_and_process_stale(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        min_idle_ms: Optional[int] = None,
    ) -> None:
        """
        Claim stale pending messages from dead consumers and process them.

        Uses XAUTOCLAIM to take over messages that have been pending for too long
        (e.g., from a crashed worker).

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
            min_idle_ms: Minimum idle time in ms (None = use default 3x check_interval)
        """
        try:
            if min_idle_ms is None:
                min_idle_ms = self._check_interval * 3 * 1000  # 3x check interval

            stale_messages = self._dlq_manager.claim_stale_messages(
                source_id=source_id,
                table_name=table_name,
                destination_id=destination_id,
                min_idle_ms=min_idle_ms,
                max_messages=self._batch_size,
                consumer_name=self._consumer_name,
            )

            if stale_messages:
                self._logger.info(
                    f"Claimed {len(stale_messages)} stale messages for "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )
                self._replay_messages_with_ids(
                    stale_messages, source_id, table_name, destination_id
                )

        except Exception as e:
            self._logger.error(
                f"Error claiming stale messages: "
                f"s{source_id}:t{table_name}:d{destination_id}: {e}",
                exc_info=True,
            )

    def _process_queue(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Process a specific DLQ queue by reading new messages.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        try:
            # Check if destination is healthy before attempting recovery
            if not self._check_destination_health(destination_id):
                self._logger.debug(
                    f"Destination {destination_id} unhealthy, skipping recovery for "
                    f"s{source_id}:t{table_name}"
                )
                return

            # Check if queue has messages (non-destructive XLEN check)
            if not self._dlq_manager.has_messages(
                source_id, table_name, destination_id
            ):
                return

            # Dequeue batch of messages via XREADGROUP
            messages_with_ids = self._dlq_manager.dequeue_batch(
                source_id,
                table_name,
                destination_id,
                max_messages=self._batch_size,
                consumer_name=self._consumer_name,
            )

            if not messages_with_ids:
                return

            self._logger.info(
                f"Processing {len(messages_with_ids)} DLQ messages for "
                f"s{source_id}:t{table_name}:d{destination_id}"
            )

            # Attempt to replay messages
            self._replay_messages_with_ids(
                messages_with_ids, source_id, table_name, destination_id
            )

        except Exception as e:
            self._logger.error(
                f"Error processing DLQ queue s{source_id}:t{table_name}:d{destination_id}: {e}",
                exc_info=True,
            )

    def _check_destination_health(self, destination_id: int) -> bool:
        """
        Check if destination is healthy and reachable.

        Args:
            destination_id: Destination ID to check

        Returns:
            True if destination is healthy
        """
        destination = self._destinations.get(destination_id)
        if not destination:
            self._logger.warning(
                f"Destination {destination_id} not found in pipeline "
                f"(orphaned DLQ - destination may have been removed). "
                f"Messages will auto-purge after {self._max_age_days} days."
            )
            return False

        try:
            # Test connection
            is_healthy = destination.test_connection()

            with self._health_check_lock:
                prev_health = self._destination_health.get(destination_id)
                self._destination_health[destination_id] = is_healthy

                # Log state changes
                if prev_health is False and is_healthy:
                    self._logger.info(
                        f"✓ Destination {destination.name} (ID={destination_id}) is now HEALTHY - "
                        f"will attempt DLQ recovery"
                    )
                elif prev_health is True and not is_healthy:
                    self._logger.warning(
                        f"✗ Destination {destination.name} (ID={destination_id}) is now UNHEALTHY"
                    )

            return is_healthy

        except Exception as e:
            self._logger.debug(
                f"Health check failed for destination {destination_id}: {e}"
            )
            with self._health_check_lock:
                self._destination_health[destination_id] = False
            return False

    def _cleanup_orphaned_queues(self, queues: list[tuple[int, str, int]]) -> None:
        """
        Clean up unused DLQ queues for destinations or tables no longer in pipeline.

        A DLQ queue is considered unused if:
        - Destination was removed from pipeline
        - Table was removed from table_metadata_list
        - Source ID doesn't match pipeline source

        Args:
            queues: List of (source_id, table_name, destination_id) tuples
        """
        try:
            # Get valid destination IDs from pipeline configuration
            valid_destination_ids = {
                pd.destination_id
                for pd in self._pipeline.destinations
                if pd.destination_id is not None
            }

            # Get valid table names from table_metadata_list for this source
            valid_tables = set(
                TableMetadataRepository.get_table_names_for_source(
                    self._pipeline.source_id
                )
            )

            pipeline_source_id = self._pipeline.source_id

            for source_id, table_name, destination_id in queues:
                should_delete = False
                reason = ""

                # Check if source_id matches pipeline
                if source_id != pipeline_source_id:
                    should_delete = True
                    reason = f"source_id {source_id} doesn't match pipeline source {pipeline_source_id}"

                # Check if destination is still in pipeline
                elif destination_id not in valid_destination_ids:
                    should_delete = True
                    reason = "destination removed from pipeline"

                # Check if table is still configured in table_metadata_list
                elif table_name not in valid_tables:
                    should_delete = True
                    reason = "table removed from pipeline configuration"

                if should_delete:
                    # Get queue size before deleting
                    queue_size = self._dlq_manager.get_queue_size(
                        source_id, table_name, destination_id
                    )

                    if queue_size > 0:
                        self._logger.warning(
                            f"Deleting unused DLQ queue ({reason}): "
                            f"s{source_id}:t{table_name}:d{destination_id} "
                            f"- discarding {queue_size} message(s)"
                        )
                    else:
                        self._logger.info(
                            f"Deleting empty unused DLQ queue ({reason}): "
                            f"s{source_id}:t{table_name}:d{destination_id}"
                        )

                    self._dlq_manager.delete_queue(
                        source_id, table_name, destination_id
                    )

        except Exception as e:
            self._logger.error(f"Error cleaning up orphaned queues: {e}", exc_info=True)

    def _replay_messages_with_ids(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Attempt to replay DLQ messages to destination.

        Groups messages by table and operation type for efficient batched writes.
        On success: ACK + DEL the messages (they're done).
        On failure: Update retry count. If max retries exceeded, discard.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        destination = self._destinations.get(destination_id)
        if not destination:
            self._logger.error(f"Destination {destination_id} not found")
            # Leave messages unacked — they'll be retried on next cycle
            return

        # Group messages by table for batch processing
        messages_by_table: dict[str, list[tuple[str, DLQMessage]]] = {}
        for msg_id, msg in messages_with_ids:
            table_key = msg.table_name
            if table_key not in messages_by_table:
                messages_by_table[table_key] = []
            messages_by_table[table_key].append((msg_id, msg))

        # Process each table's messages in operation-type batches
        for tbl_name, table_messages in messages_by_table.items():
            self._replay_table_messages_batched(
                table_messages, destination, source_id, tbl_name, destination_id
            )

    def _replay_table_messages(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        destination: BaseDestination,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Replay messages for a specific table (legacy single-batch method).

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            destination: Destination to write to
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        # Delegate to the batched implementation
        self._replay_table_messages_batched(
            messages_with_ids, destination, source_id, table_name, destination_id
        )

    def _replay_table_messages_batched(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        destination: BaseDestination,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Replay messages for a specific table with operation-type batching (C2).

        Groups messages by CDC operation type (c/u/d/r) and submits each group
        as a consolidated batch write. This reduces the number of write_batch()
        calls and allows the destination to optimize bulk operations.

        Order is preserved within each operation group.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            destination: Destination to write to
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        if not messages_with_ids:
            return

        # All messages should have same routing info
        first_msg = messages_with_ids[0][1]
        table_name_target = first_msg.table_name_target

        # Fetch FRESH table_sync config from database instead of using stale
        # config stored in DLQ message. This ensures updated custom_sql/filter_sql
        # are applied during recovery.
        table_sync_id = first_msg.table_sync_config.get("id")
        if table_sync_id:
            try:
                fresh_table_sync = TableSyncRepository.get_by_id(table_sync_id)
                if fresh_table_sync:
                    table_sync = fresh_table_sync
                    self._logger.debug(
                        f"Using fresh table_sync config from DB (id={table_sync_id})"
                    )
                else:
                    # Table sync was deleted — fall back to stored config
                    table_sync = self._create_table_sync_from_config(
                        first_msg.table_sync_config
                    )
                    self._logger.warning(
                        f"Table sync {table_sync_id} not found in DB, "
                        f"using stale config from DLQ message"
                    )
            except Exception as e:
                # DB fetch failed — fall back to stored config
                self._logger.warning(
                    f"Failed to fetch fresh table_sync {table_sync_id} from DB: {e}. "
                    f"Using stale config from DLQ message"
                )
                table_sync = self._create_table_sync_from_config(
                    first_msg.table_sync_config
                )
        else:
            # Old message format without table_sync_id — use stored config
            table_sync = self._create_table_sync_from_config(
                first_msg.table_sync_config
            )

        # --- C2: Group messages by operation type for batched writes ---
        # Operations: c=create, u=update, d=delete, r=read/snapshot
        # Process order: deletes first, then creates/reads, then updates
        # This ordering minimizes conflicts during replay.
        op_order = ["d", "r", "c", "u"]
        messages_by_op: dict[str, list[tuple[str, DLQMessage]]] = {}
        for msg_id, msg in messages_with_ids:
            op = msg.cdc_record.operation if msg.cdc_record else "c"
            if op not in messages_by_op:
                messages_by_op[op] = []
            messages_by_op[op].append((msg_id, msg))

        # Track all successfully replayed message IDs across all batches
        all_success_ids: list[str] = []
        all_failed: list[tuple[str, DLQMessage]] = []
        total_written = 0

        try:
            # Ensure destination is initialized before writing
            if not destination._is_initialized:
                self._logger.debug(
                    f"Initializing destination {destination.name} before replay"
                )
                destination.initialize()
            else:
                self._logger.debug(
                    f"Checking connection health for destination {destination.name}"
                )
                destination.initialize(force_reconnect=False)

            # Process each operation type batch
            for op in op_order:
                if op not in messages_by_op:
                    continue

                op_messages = messages_by_op[op]
                cdc_records = [msg.cdc_record for _, msg in op_messages]
                msg_ids = [msg_id for msg_id, _ in op_messages]

                try:
                    written = destination.write_batch(cdc_records, table_sync)
                    total_written += written
                    all_success_ids.extend(msg_ids)

                    self._logger.debug(
                        f"Batched replay op={op}: {written}/{len(cdc_records)} records "
                        f"to {destination.name} for table {table_name}"
                    )
                except Exception as batch_e:
                    self._logger.warning(
                        f"Batch replay failed for op={op} "
                        f"({len(op_messages)} messages): {batch_e}"
                    )
                    all_failed.extend(op_messages)

            # Also process any operation types not in op_order
            for op, op_messages in messages_by_op.items():
                if op in op_order:
                    continue
                cdc_records = [msg.cdc_record for _, msg in op_messages]
                msg_ids = [msg_id for msg_id, _ in op_messages]

                try:
                    written = destination.write_batch(cdc_records, table_sync)
                    total_written += written
                    all_success_ids.extend(msg_ids)
                except Exception as batch_e:
                    self._logger.warning(
                        f"Batch replay failed for op={op}: {batch_e}"
                    )
                    all_failed.extend(op_messages)

            if all_success_ids:
                self._logger.info(
                    f"✓ Batched replay: {total_written} records replayed to "
                    f"{destination.name} for table {table_name} "
                    f"({len(all_success_ids)} messages ACKed, {len(all_failed)} failed)"
                )

                # Clear error flags
                pipeline_destination_id = first_msg.table_sync_config.get(
                    "pipeline_destination_id"
                )
                ts_id = first_msg.table_sync_config.get("id")

                if pipeline_destination_id:
                    PipelineDestinationRepository.update_error(
                        pipeline_destination_id, False
                    )
                if ts_id:
                    TableSyncRepository.update_error(ts_id, False)

                # Acknowledge successful messages
                self._dlq_manager.acknowledge(
                    source_id, table_name, destination_id, all_success_ids
                )

            # Handle retry for failed batches
            if all_failed:
                self._handle_retry(all_failed, source_id, table_name, destination_id)

            # Check if queue is now empty and clean up
            if all_success_ids and not self._dlq_manager.has_messages(
                source_id, table_name, destination_id
            ):
                self._logger.info(
                    f"Queue empty after batched replay, cleaning up: "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )
                self._dlq_manager.delete_queue(source_id, table_name, destination_id)

        except (DestinationException, Exception) as e:
            is_dest_error = isinstance(e, DestinationException)
            log_method = self._logger.warning if is_dest_error else self._logger.error

            log_method(
                f"Failed to replay DLQ messages to {destination.name} for table {table_name}: {e}",
                exc_info=not is_dest_error,
            )

            # Update retry count for ALL messages on total failure
            self._handle_retry(messages_with_ids, source_id, table_name, destination_id)

    def _handle_retry(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Handle retry logic for failed messages.

        Increments retry_count. If max retries exceeded, acknowledges and discards
        the message. Otherwise, creates a new entry with updated retry count and
        removes the old one.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        ids_to_discard = []
        for msg_id, msg in messages_with_ids:
            msg.retry_count += 1

            if msg.retry_count >= self._max_retry_count:
                # Max retries exceeded — discard message
                self._logger.warning(
                    f"Discarding DLQ message after {msg.retry_count} retries: "
                    f"s{source_id}:t{table_name}:d{destination_id} "
                    f"operation={msg.cdc_record.operation}, key={msg.cdc_record.key}"
                )
                ids_to_discard.append(msg_id)
            else:
                # Replace with updated retry count (XADD new + XACK/XDEL old)
                self._dlq_manager.update_message_retry(
                    source_id, table_name, destination_id, msg_id, msg
                )

        # Discard messages that exceeded max retries
        if ids_to_discard:
            self._dlq_manager.acknowledge(
                source_id, table_name, destination_id, ids_to_discard
            )

    def _create_table_sync_from_config(
        self,
        config: dict,
    ) -> PipelineDestinationTableSync:
        """
        Create PipelineDestinationTableSync from stored config.

        This is a lightweight object just for passing to write_batch.
        Not a full ORM object.
        """

        # Create a simple object that has the required attributes
        class TableSyncStub:
            def __init__(self, config_dict):
                self.id = config_dict.get("id")
                self.pipeline_destination_id = config_dict.get(
                    "pipeline_destination_id"
                )
                self.table_name = config_dict.get("table_name")
                self.table_name_target = config_dict.get("table_name_target")
                self.filter_sql = config_dict.get("filter_sql")
                self.custom_sql = config_dict.get("custom_sql")

        return TableSyncStub(config)

    def get_stats(self) -> dict:
        """
        Get recovery worker statistics.

        Returns:
            Dict with statistics
        """
        queues = self._dlq_manager.list_queues()

        # Gather queue sizes
        queue_stats = []
        for source_id, table_name, dest_id in queues:
            size = self._dlq_manager.get_queue_size(source_id, table_name, dest_id)
            queue_stats.append(
                {
                    "source_id": source_id,
                    "table_name": table_name,
                    "destination_id": dest_id,
                    "size": size,
                }
            )

        with self._health_check_lock:
            health_stats = dict(self._destination_health)

        return {
            "running": self._running,
            "pipeline_id": self._pipeline.id,
            "pipeline_name": self._pipeline.name,
            "check_interval": self._check_interval,
            "batch_size": self._batch_size,
            "total_queues": len(queues),
            "queues": queue_stats,
            "destination_health": health_stats,
        }
