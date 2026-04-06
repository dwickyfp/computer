"""
Record routing for CDC records.

This module owns destination fan-out, error handling, monitoring updates, and
DLQ enqueueing for normalized ``CDCRecord`` batches, independent of the source
transport.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Optional

from destinations.base import CDCRecord, BaseDestination
from core.models import Pipeline, PipelineDestination, PipelineDestinationTableSync
from core.repository import (
    DataFlowRepository,
    TableSyncRepository,
    PipelineDestinationRepository,
)
from core.exceptions import DestinationException
from core.dlq_manager import DLQManager
from core.notification import NotificationLogRepository, NotificationLogCreate
from core.error_sanitizer import sanitize_for_db, sanitize_for_log
from core.runtime_metrics import observe

logger = logging.getLogger(__name__)


@dataclass
class RoutingInfo:
    """Information for routing a record to a destination."""

    pipeline_destination: PipelineDestination
    table_sync: PipelineDestinationTableSync
    destination: BaseDestination


class RecordRouter:
    """Routes normalized CDC records to configured destinations."""

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: Optional[DLQManager] = None,
        shutdown_event: Optional[threading.Event] = None,
    ):
        self._pipeline = pipeline
        self._destinations = destinations
        self._dlq_manager = dlq_manager
        self._shutdown_event = shutdown_event or threading.Event()
        self._logger = logging.getLogger(f"{__name__}.{pipeline.name}")
        self._routing_table: dict[str, list[RoutingInfo]] = {}
        self._build_routing_table()

    def _build_routing_table(self) -> None:
        for pd in self._pipeline.destinations:
            destination = self._destinations.get(pd.destination_id)
            if not destination:
                continue

            for table_sync in pd.table_syncs:
                self._routing_table.setdefault(table_sync.table_name, []).append(
                    RoutingInfo(
                        pipeline_destination=pd,
                        table_sync=table_sync,
                        destination=destination,
                    )
                )

    def route_records(self, table_name: str, records: list[CDCRecord]) -> None:
        routing_list = self._routing_table.get(table_name)
        if not routing_list:
            return

        if len(routing_list) == 1:
            self._process_single_destination(routing_list[0], table_name, records)
            return

        with ThreadPoolExecutor(max_workers=len(routing_list)) as executor:
            futures = {
                executor.submit(
                    self._process_single_destination, routing, table_name, records
                ): routing
                for routing in routing_list
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    routing = futures[future]
                    self._logger.error(
                        "Unexpected error writing to %s: %s",
                        routing.destination.name,
                        exc,
                        exc_info=True,
                    )

    def route_batches(self, records_by_table: dict[str, list[CDCRecord]]) -> None:
        for table_name, table_records in records_by_table.items():
            started = time.perf_counter()
            self.route_records(table_name, table_records)
            observe(
                "record_router.route_duration",
                (time.perf_counter() - started) * 1000.0,
                unit="ms",
                pipeline_id=str(self._pipeline.id),
                table_name=table_name,
            )

    def _process_single_destination(
        self,
        routing: RoutingInfo,
        table_name: str,
        records: list[CDCRecord],
    ) -> None:
        try:
            if self._shutdown_event.is_set():
                return

            dest_type = (
                routing.destination._config.type
                if hasattr(routing.destination, "_config")
                else "unknown"
            )
            write_guard = nullcontext()
            if self._dlq_manager:
                write_guard = self._dlq_manager.write_guard(
                    routing.destination.destination_id, table_name
                )

            with write_guard:
                written = routing.destination.write_batch(records, routing.table_sync)
                self._track_written_versions_if_safe(
                    destination_id=routing.destination.destination_id,
                    table_name=table_name,
                    records=records,
                    written=written,
                )

            if written > 0:
                if dest_type.lower() == "snowflake":
                    target_table = routing.table_sync.table_name_target.upper()
                    monitoring_table_name = (
                        target_table
                        if target_table.startswith("LANDING_")
                        else f"LANDING_{target_table}"
                    )
                else:
                    monitoring_table_name = routing.table_sync.table_name_target
                self._update_monitoring(routing, monitoring_table_name, written)

            if routing.pipeline_destination.is_error or routing.table_sync.is_error:
                PipelineDestinationRepository.update_error(
                    routing.pipeline_destination.id, False
                )
                TableSyncRepository.update_error(routing.table_sync.id, False)
                routing.pipeline_destination.is_error = False
                routing.pipeline_destination.error_message = None
                routing.table_sync.is_error = False
                routing.table_sync.error_message = None

        except DestinationException as exc:
            db_error_msg = sanitize_for_db(
                exc, routing.destination.name, routing.destination._config.type
            )
            if self._dlq_manager:
                self._enqueue_to_dlq(records, routing, db_error_msg)
            self._create_destination_failure_notification(
                routing, table_name, db_error_msg, "DESTINATION_ERROR"
            )
            TableSyncRepository.update_error(routing.table_sync.id, True, db_error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, db_error_msg
            )
            routing.table_sync.is_error = True
            routing.table_sync.error_message = db_error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = db_error_msg

        except Exception as exc:
            self._logger.error(
                "Unexpected error writing to destination %s for table %s: %s",
                routing.destination.name,
                table_name,
                sanitize_for_log(exc),
                exc_info=True,
            )
            db_error_msg = sanitize_for_db(
                exc, routing.destination.name, routing.destination._config.type
            )
            if self._dlq_manager:
                self._enqueue_to_dlq(records, routing, db_error_msg)
            self._create_destination_failure_notification(
                routing, table_name, db_error_msg, "CONNECTION_ERROR"
            )
            TableSyncRepository.update_error(routing.table_sync.id, True, db_error_msg)
            PipelineDestinationRepository.update_error(
                routing.pipeline_destination.id, True, db_error_msg
            )
            routing.table_sync.is_error = True
            routing.table_sync.error_message = db_error_msg
            routing.pipeline_destination.is_error = True
            routing.pipeline_destination.error_message = db_error_msg

    def _track_written_versions_if_safe(
        self,
        destination_id: int,
        table_name: str,
        records: list[CDCRecord],
        written: int,
    ) -> None:
        """Track versions only when the destination confirms the full batch wrote."""
        if not self._dlq_manager or written <= 0:
            return

        if written != len(records):
            self._logger.debug(
                "Skipping version tracking for partial write to d%s:t%s (%s/%s)",
                destination_id,
                table_name,
                written,
                len(records),
            )
            return

        try:
            self._dlq_manager.track_written_versions(
                destination_id=destination_id,
                table_name=table_name,
                records=records,
            )
        except Exception:
            pass

    def _update_monitoring(
        self,
        routing: RoutingInfo,
        table_name: str,
        record_count: int,
    ) -> None:
        try:
            DataFlowRepository.increment_count(
                pipeline_id=self._pipeline.id,
                pipeline_destination_id=routing.pipeline_destination.id,
                source_id=self._pipeline.source_id,
                table_sync_id=routing.table_sync.id,
                table_name=table_name,
                count=record_count,
            )
        except Exception as exc:
            self._logger.warning("Failed to update monitoring: %s", exc)

    def _enqueue_to_dlq(
        self,
        records: list[CDCRecord],
        routing: RoutingInfo,
        error_message: str,
    ) -> None:
        if not self._dlq_manager:
            return
        try:
            self._dlq_manager.enqueue_batch(
                pipeline_id=self._pipeline.id,
                source_id=self._pipeline.source_id,
                destination_id=routing.destination.destination_id,
                table_name_target=routing.table_sync.table_name_target,
                cdc_records=records,
                table_sync=routing.table_sync,
                error_message=error_message,
            )
        except Exception as exc:
            self._logger.error(
                "Failed to enqueue batch to DLQ (%s records): %s",
                len(records),
                exc,
                exc_info=True,
            )

    def _create_destination_failure_notification(
        self,
        routing: RoutingInfo,
        table_name: str,
        error_message: str,
        error_type: str,
    ) -> None:
        try:
            NotificationLogRepository.create(
                NotificationLogCreate(
                    title=f"{error_type}: {routing.destination.name}",
                    message=(
                        f"Pipeline {self._pipeline.name} failed writing table "
                        f"{table_name} to destination {routing.destination.name}: "
                        f"{error_message}"
                    ),
                    category="pipeline_destination",
                    level="ERROR",
                    is_force_sent=True,
                )
            )
        except Exception as exc:
            self._logger.warning("Failed to create notification log: %s", exc)
