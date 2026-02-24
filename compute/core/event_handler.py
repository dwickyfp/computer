"""
Debezium change event handler for processing CDC records.

Parses Debezium events and routes them to appropriate destinations.
"""

import json
import logging
from typing import Any, Optional
from dataclasses import dataclass

from pydbzengine import ChangeEvent, BasePythonChangeHandler

from destinations.base import CDCRecord, BaseDestination
from core.models import (
    Pipeline,
    PipelineDestination,
    PipelineDestinationTableSync,
)
from core.repository import (
    DataFlowRepository,
    TableSyncRepository,
    PipelineDestinationRepository,
)
from core.exceptions import DestinationException
from core.dlq_manager import DLQManager
from core.notification import NotificationLogRepository, NotificationLogCreate
from core.error_sanitizer import sanitize_for_db, sanitize_for_log

logger = logging.getLogger(__name__)


@dataclass
class RoutingInfo:
    """Information for routing a record to a destination."""

    pipeline_destination: PipelineDestination
    table_sync: PipelineDestinationTableSync
    destination: BaseDestination


class CDCEventHandler(BasePythonChangeHandler):
    """
    Debezium change event handler that routes CDC records to destinations.

    Handles parsing of Debezium events and routing to configured destinations
    based on pipeline configuration. Failed writes are automatically routed to DLQ.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: Optional[DLQManager] = None,
    ):
        """
        Initialize CDC event handler.

        Args:
            pipeline: Pipeline configuration with destinations loaded
            destinations: Dict mapping destination_id to BaseDestination instances
            dlq_manager: Optional DLQ manager for handling failed writes
        """
        self._pipeline = pipeline
        self._destinations = destinations
        self._dlq_manager = dlq_manager
        self._logger = logging.getLogger(f"{__name__}.{pipeline.name}")

        # Build routing table: table_name -> list of RoutingInfo
        self._routing_table: dict[str, list[RoutingInfo]] = {}
        self._build_routing_table()

    def _build_routing_table(self) -> None:
        """Build routing table from pipeline configuration."""
        self._logger.info(f"Building routing table for pipeline {self._pipeline.name}")
        self._logger.info(
            f"Pipeline has {len(self._pipeline.destinations)} destination(s)"
        )

        for pd in self._pipeline.destinations:
            self._logger.info(
                f"Processing pipeline_destination {pd.id} -> destination_id {pd.destination_id}"
            )
            destination = self._destinations.get(pd.destination_id)
            if not destination:
                self._logger.warning(
                    f"Destination {pd.destination_id} not found for pipeline {self._pipeline.name}"
                )
                continue

            self._logger.info(
                f"Destination {pd.destination_id} has {len(pd.table_syncs)} table_sync(s)"
            )
            for table_sync in pd.table_syncs:
                table_name = table_sync.table_name
                self._logger.info(
                    f"  Adding routing for table: '{table_name}' -> target: '{table_sync.table_name_target}'"
                )

                if table_name not in self._routing_table:
                    self._routing_table[table_name] = []

                self._routing_table[table_name].append(
                    RoutingInfo(
                        pipeline_destination=pd,
                        table_sync=table_sync,
                        destination=destination,
                    )
                )

        self._logger.info(
            f"Built routing table with {len(self._routing_table)} tables: {list(self._routing_table.keys())}"
        )

    def _parse_destination_to_table_name(self, destination: str) -> str:
        """
        Extract table name from Debezium destination topic.

        Format: topic_prefix.schema.table_name
        Example: rosetta_my-pipeline.public.tbl_sales_stream_company -> tbl_sales_stream_company

        Args:
            destination: Debezium topic/destination (may be JPype Java string)

        Returns:
            Table name (last segment after splitting by '.')
        """
        # Convert JPype Java string to Python string
        dest_str = str(destination) if destination is not None else ""

        if not dest_str:
            self._logger.debug("Empty destination received, skipping")
            return ""

        # Split by dot to extract table name
        parts = dest_str.split(".")
        if len(parts) == 0:
            self._logger.warning(
                f"Could not parse destination '{dest_str}', no parts after split"
            )
            return ""

        # Skip non-table destinations (heartbeats, transaction commits, etc.)
        # Valid table destinations have format: topic_prefix.schema.table_name (3 parts)
        if len(parts) < 3:
            self._logger.debug(f"Skipping non-table destination: '{dest_str}'")
            return ""

        table_name = parts[-1]  # Always extract last part as table name
        self._logger.debug(
            f"Parsed destination '{dest_str}' -> table_name '{table_name}'"
        )
        return table_name

    def _parse_record(self, record: ChangeEvent) -> Optional[CDCRecord]:
        """
        Parse Debezium ChangeEvent into CDCRecord.

        Args:
            record: Raw Debezium change event

        Returns:
            Parsed CDCRecord or None if parsing fails
        """
        try:
            destination = record.destination()
            key_data = record.key()
            value_data = record.value()

            # Ensure we have Python strings (handle java.lang.String from JPype)
            # Parse JSON strings with null safety
            key_obj = json.loads(str(key_data)) if key_data is not None else {}
            value_obj = json.loads(str(value_data)) if value_data is not None else {}

            # Extract payload from Debezium format
            payload = value_obj.get("payload", {})
            op = payload.get("op")

            # Extract key payload
            if isinstance(key_obj, dict) and "payload" in key_obj:
                key = key_obj["payload"]
            else:
                key = key_obj

            # Extract value based on operation type
            if op in ("c", "u", "r"):  # create, update, read (snapshot)
                value = payload.get("after", {})
            elif op == "d":  # delete
                value = payload.get("before", {})
            elif op == "m":  # message (heartbeat)
                # Skip heartbeat messages
                return None
            else:
                value = payload if payload else {}

            # Get table name from destination topic
            table_name = self._parse_destination_to_table_name(destination)
            if not table_name:
                # Skip records with invalid/empty destination
                return None

            # Extract schema if available
            schema = value_obj.get("schema")

            return CDCRecord(
                operation=op or "u",
                table_name=table_name,
                key=key if isinstance(key, dict) else {},
                value=value if isinstance(value, dict) else {},
                schema=schema,
                timestamp=payload.get("ts_ms"),
            )

        except Exception as e:
            self._logger.error(f"Failed to parse record: {e}", exc_info=True)
            self._logger.error(
                f"Record details - destination: {record.destination() if record else 'N/A'}"
            )
            return None

    def handleJsonBatch(self, records: list[ChangeEvent]) -> None:
        """
        Handle a batch of Debezium change events.

        Routes each record to configured destinations based on table name.

        Args:
            records: List of Debezium change events
        """

        # Group records by table
        records_by_table: dict[str, list[CDCRecord]] = {}
        skipped_count = 0
        ops_seen = []

        for record in records:
            cdc_record = self._parse_record(record)
            if cdc_record is None:
                skipped_count += 1
                # Log what's being skipped for debugging
                try:
                    value_data = record.value()
                    if value_data:
                        value_obj = json.loads(str(value_data))
                        op = value_obj.get("payload", {}).get("op", "unknown")
                        ops_seen.append(op)
                    else:
                        ops_seen.append("empty_value")
                except Exception as parse_ex:
                    ops_seen.append(f"parse_error:{parse_ex}")
                continue

            table_name = cdc_record.table_name
            ops_seen.append(cdc_record.operation)
            if table_name not in records_by_table:
                records_by_table[table_name] = []
            records_by_table[table_name].append(cdc_record)

        # Process each table's records
        for table_name, table_records in records_by_table.items():
            self._process_table_records(table_name, table_records)

    def _process_table_records(
        self,
        table_name: str,
        records: list[CDCRecord],
    ) -> None:
        """
        Process records for a specific table.

        Routes to all configured destinations for this table.
        Each destination is processed independently - if one fails, others continue.

        Args:
            table_name: Source table name
            records: CDC records for this table
        """
        routing_list = self._routing_table.get(table_name)

        if not routing_list:
            return

        # Process each destination independently with individual error handling
        for routing in routing_list:
            self._process_single_destination(routing, table_name, records)

    def _process_single_destination(
        self,
        routing: RoutingInfo,
        table_name: str,
        records: list[CDCRecord],
    ) -> None:
        """
        Process records for a single destination with isolated error handling.

        If this destination fails, it won't affect other destinations in the same pipeline.
        Error state is tracked in pipelines_destination and pipelines_destination_table_sync.

        Args:
            routing: Routing information for the destination
            table_name: Source table name
            records: CDC records to write
        """
        try:
            dest_type = (
                routing.destination._config.type
                if hasattr(routing.destination, "_config")
                else "unknown"
            )
            dest_name = routing.destination.name

            # Write to destination - this is where connection/table errors can occur
            written = routing.destination.write_batch(records, routing.table_sync)

            # Update data flow monitoring
            if written > 0:
                # For Snowflake, use landing table name; for PostgreSQL, use target table name
                if dest_type.lower() == "snowflake":
                    target_table = routing.table_sync.table_name_target.upper()
                    if target_table.startswith("LANDING_"):
                        monitoring_table_name = target_table
                    else:
                        monitoring_table_name = f"LANDING_{target_table}"
                else:
                    # For PostgreSQL and other destinations, use the target table name
                    monitoring_table_name = routing.table_sync.table_name_target

                self._update_monitoring(routing, monitoring_table_name, written)

            # Clear error state if previously failed - destination is now healthy
            if routing.pipeline_destination.is_error or routing.table_sync.is_error:
                PipelineDestinationRepository.update_error(
                    routing.pipeline_destination.id, False
                )
                TableSyncRepository.update_error(routing.table_sync.id, False)
                # Update in-memory state
                routing.pipeline_destination.is_error = False
                routing.pipeline_destination.error_message = None
                routing.table_sync.is_error = False
                routing.table_sync.error_message = None

        except DestinationException as e:
            # Destination-specific error (e.g., table not exists, schema mismatch)
            log_msg = f"Destination error: {sanitize_for_log(e)}"

            # Sanitize error for database storage and user display
            db_error_msg = sanitize_for_db(
                e, routing.destination.name, routing.destination._config.type
            )

            # Enqueue failed records to DLQ if available
            if self._dlq_manager:
                self._enqueue_to_dlq(records, routing, db_error_msg)

            # Insert notification with is_force_sent=True
            self._create_destination_failure_notification(
                routing, table_name, db_error_msg, "DESTINATION_ERROR"
            )

            # Update error state for both table sync and pipeline destination
            TableSyncRepository.update_error(routing.table_sync.id, True, db_error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, db_error_msg
            )

            # Update in-memory state
            routing.table_sync.is_error = True
            routing.table_sync.error_message = db_error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = db_error_msg

        except Exception as e:
            # Unexpected error (connection issues, authentication, etc.)
            log_msg = f"Unexpected error: {sanitize_for_log(e)}"
            self._logger.error(
                f"✗ Unexpected error writing to destination {routing.destination.name} "
                f"for table {table_name}: {log_msg}",
                exc_info=True,
            )

            # Sanitize error for database storage and user display
            db_error_msg = sanitize_for_db(
                e, routing.destination.name, routing.destination._config.type
            )

            # Enqueue failed records to DLQ if available
            if self._dlq_manager:
                self._enqueue_to_dlq(records, routing, db_error_msg)

            # Insert notification with is_force_sent=True (connection issue)
            self._create_destination_failure_notification(
                routing, table_name, db_error_msg, "CONNECTION_ERROR"
            )

            # Update error state for both table sync and pipeline destination
            TableSyncRepository.update_error(routing.table_sync.id, True, db_error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, db_error_msg
            )

            # Update in-memory state
            routing.table_sync.is_error = True
            routing.table_sync.error_message = db_error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = db_error_msg

    def _update_monitoring(
        self,
        routing: RoutingInfo,
        table_name: str,
        record_count: int,
    ) -> None:
        """
        Update data flow record monitoring.

        Args:
            routing: Routing information
            table_name: Table name
            record_count: Number of records written
        """
        try:
            DataFlowRepository.increment_count(
                pipeline_id=self._pipeline.id,
                pipeline_destination_id=routing.pipeline_destination.id,
                source_id=self._pipeline.source_id,
                table_sync_id=routing.table_sync.id,
                table_name=table_name,
                count=record_count,
            )
        except Exception as e:
            self._logger.warning(f"Failed to update monitoring: {e}")

    def _enqueue_to_dlq(
        self,
        records: list[CDCRecord],
        routing: RoutingInfo,
        error_message: str,
    ) -> None:
        """
        Enqueue failed records to dead letter queue.

        Args:
            records: CDC records that failed to write
            routing: Routing information
            error_message: Error message describing the failure
        """
        if not self._dlq_manager:
            return

        for record in records:
            try:
                self._dlq_manager.enqueue(
                    pipeline_id=self._pipeline.id,
                    source_id=self._pipeline.source_id,
                    destination_id=routing.destination.destination_id,
                    table_name=record.table_name,
                    table_name_target=routing.table_sync.table_name_target,
                    cdc_record=record,
                    table_sync=routing.table_sync,
                    error_message=error_message,
                )
            except Exception as e:
                self._logger.error(
                    f"Failed to enqueue record to DLQ: {e}",
                    exc_info=True,
                )

    def _create_destination_failure_notification(
        self,
        routing: RoutingInfo,
        table_name: str,
        error_message: str,
        error_type: str,
    ) -> None:
        """
        Create notification log entry for destination failure.

        Anti-spam logic: Only creates notification if:
        1. No previous notification exists, OR
        2. Last notification was created >5 minutes ago, OR
        3. Last notification has is_sent=True or is_read=True

        Args:
            routing: Routing information
            table_name: Table name that failed
            error_message: Error message
            error_type: Type of error (CONNECTION_ERROR, DESTINATION_ERROR)
        """
        try:
            from datetime import datetime, timezone, timedelta
            from core.database import get_db_connection, return_db_connection

            # Create unique key for this destination+table combination
            key_notification = f"pipeline_{self._pipeline.id}_dest_{routing.destination.destination_id}_table_{table_name}_{error_type}"

            # Check if we should create notification (anti-spam logic)
            should_create = False
            conn = None

            try:
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    # Get last notification with this key
                    cursor.execute(
                        """
                        SELECT created_at, is_sent, is_read
                        FROM notification_log
                        WHERE key_notification = %s AND is_deleted = FALSE
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (key_notification,),
                    )
                    result = cursor.fetchone()

                    if not result:
                        # No previous notification, create new one
                        should_create = True
                    else:
                        last_created_at, is_sent, is_read = result

                        # Check if last notification was sent or read
                        if is_sent or is_read:
                            should_create = True
                            self._logger.debug(
                                f"Creating notification: last notification was {'sent' if is_sent else 'read'}"
                            )
                        else:
                            # Check 5-minute gap
                            now = datetime.now(timezone.utc)
                            # Ensure last_created_at is timezone-aware
                            if last_created_at.tzinfo is None:
                                last_created_at = last_created_at.replace(
                                    tzinfo=timezone.utc
                                )

                            time_diff = now - last_created_at
                            if time_diff >= timedelta(minutes=5):
                                should_create = True
                                self._logger.debug(
                                    f"Creating notification: {time_diff.total_seconds():.0f}s since last notification (>5min)"
                                )
                            else:
                                self._logger.debug(
                                    f"Skipping notification: only {time_diff.total_seconds():.0f}s since last notification (<5min), "
                                    f"and not yet sent/read"
                                )

            finally:
                if conn:
                    return_db_connection(conn)

            # If anti-spam conditions not met, skip notification
            if not should_create:
                return

            # Determine notification type and title based on error
            if (
                "connection" in error_message.lower()
                or error_type == "CONNECTION_ERROR"
            ):
                notif_type = "ERROR"
                title = f"Destination Connection Failed: {routing.destination.name}"
                message = (
                    f"Failed to connect to destination '{routing.destination.name}' "
                    f"for table '{table_name}' in pipeline '{self._pipeline.name}'. \n\n"
                    f"Error: {error_message}\n\n"
                    f"CDC events are being stored in Dead Letter Queue (DLQ) and will be "
                    f"automatically replayed when the destination recovers."
                )
            else:
                notif_type = "WARNING"
                title = f"Destination Error: {routing.destination.name}"
                message = (
                    f"Error writing to destination '{routing.destination.name}' "
                    f"for table '{table_name}' in pipeline '{self._pipeline.name}'. \n\n"
                    f"Error: {error_message}\n\n"
                    f"CDC events are being stored in DLQ for recovery."
                )

            notif_repo = NotificationLogRepository()
            notification = NotificationLogCreate(
                key_notification=key_notification,
                title=title,
                message=message,
                type=notif_type,
                is_force_sent=True,  # Force notification for connection issues
            )

            notif_id = notif_repo.upsert_notification_by_key(notification)
            if notif_id:
                self._logger.info(
                    f"Created notification (ID={notif_id}) for destination failure: {routing.destination.name}"
                )

        except Exception as e:
            self._logger.error(
                f"Failed to create notification for destination failure: {e}",
                exc_info=True,
            )
