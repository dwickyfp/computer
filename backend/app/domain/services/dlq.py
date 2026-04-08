"""
DLQ service for the DLQ Manager page.

Reads Redis Streams non-destructively for queue/message previews and
supports destructive discard actions for selected rows, queues, or pipelines.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import redis
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.config import get_settings
from app.core.exceptions import EntityNotFoundError, ValidationError
from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.models.pipeline import Pipeline, PipelineDestination
from app.domain.schemas.dlq import (
    DLQDiscardMessagesRequest,
    DLQDiscardResponse,
    DLQMessageResponse,
    DLQMessagesResponse,
    DLQPipelineDiscardResponse,
    DLQQueueIdentifier,
    DLQQueueListResponse,
    DLQQueueSummary,
)
from app.infrastructure.redis import get_redis

logger = get_logger(__name__)


class DLQService:
    """Business logic for DLQ inspection and discard operations."""

    def __init__(
        self,
        db: Session,
        redis_client: redis.Redis | None = None,
    ):
        self.db = db
        self.settings = get_settings()
        self.redis = redis_client or get_redis()
        self.key_prefix = self.settings.dlq_key_prefix
        self.consumer_group = self.settings.dlq_consumer_group
        self._stream_key_pattern = re.compile(
            rf"^{re.escape(self.key_prefix)}:s(\d+):t(.+):d(\d+)$"
        )

    def list_queues(
        self,
        pipeline_id: int | None = None,
        destination_id: int | None = None,
        search: str | None = None,
        include_empty: bool = False,
    ) -> DLQQueueListResponse:
        """List DLQ queues enriched with pipeline/source/destination metadata."""
        queue_keys = self._scan_queue_keys()
        parsed_queues = []
        source_ids: set[int] = set()
        destination_ids: set[int] = set()
        for key in queue_keys:
            parsed = self._parse_stream_key(key)
            if not parsed:
                continue
            source_id, table_name, dest_id = parsed
            parsed_queues.append((key, source_id, table_name, dest_id))
            source_ids.add(source_id)
            destination_ids.add(dest_id)

        pipelines_by_source = self._load_pipelines_by_source(source_ids)
        destination_names = self._load_destination_names(destination_ids)
        search_term = search.strip().lower() if search else ""

        items: list[DLQQueueSummary] = []
        for stream_key, source_id, table_name, dest_id in parsed_queues:
            pipeline = pipelines_by_source.get(source_id)
            if pipeline_id is not None and (pipeline is None or pipeline.id != pipeline_id):
                continue
            if destination_id is not None and dest_id != destination_id:
                continue

            message_count = self._safe_xlen(stream_key)
            if not include_empty and message_count == 0:
                continue

            newest = self._read_boundary_message(stream_key, newest=True)
            oldest = self._read_boundary_message(stream_key, newest=False)

            summary = DLQQueueSummary(
                pipeline_id=pipeline.id if pipeline else None,
                pipeline_name=pipeline.name if pipeline else None,
                source_id=source_id,
                source_name=pipeline.source.name if pipeline and pipeline.source else None,
                destination_id=dest_id,
                destination_name=destination_names.get(dest_id),
                table_name=table_name,
                table_name_target=(
                    newest.table_name_target
                    if newest and newest.table_name_target
                    else oldest.table_name_target if oldest else None
                ),
                message_count=message_count,
                oldest_failed_at=oldest.first_failed_at if oldest else None,
                newest_failed_at=newest.first_failed_at if newest else None,
            )

            if search_term and search_term not in self._queue_search_blob(summary):
                continue

            items.append(summary)

        items.sort(
            key=lambda item: (
                item.pipeline_name or "",
                item.destination_name or "",
                item.table_name,
            )
        )

        return DLQQueueListResponse(
            items=items,
            total_messages=sum(item.message_count for item in items),
            total_queues=len(items),
            total_pipelines=len({item.pipeline_id for item in items if item.pipeline_id}),
            total_destinations=len({item.destination_id for item in items}),
        )

    def list_messages(
        self,
        source_id: int,
        destination_id: int,
        table_name: str,
        before_id: str | None = None,
        limit: int = 50,
    ) -> DLQMessagesResponse:
        """List queue messages newest-first without consuming them."""
        stream_key = self._stream_key(source_id, table_name, destination_id)
        max_bound = f"({before_id}" if before_id else "+"
        entries = self.redis.xrevrange(stream_key, max=max_bound, min="-", count=limit)

        items: list[DLQMessageResponse] = []
        for entry_id, entry_data in entries:
            parsed = self._parse_message(entry_id, entry_data)
            if parsed:
                items.append(parsed)

        next_before_id = items[-1].message_id if len(items) == limit else None
        return DLQMessagesResponse(
            items=items,
            next_before_id=next_before_id,
            total_count=self._safe_xlen(stream_key),
        )

    def discard_messages(
        self,
        request: DLQDiscardMessagesRequest,
    ) -> DLQDiscardResponse:
        """Discard selected messages from a queue."""
        if not request.message_ids:
            raise ValidationError(
                message="At least one message id is required",
                details={"field": "message_ids"},
            )

        stream_key = self._stream_key(
            request.source_id,
            request.table_name,
            request.destination_id,
        )
        try:
            self.redis.xack(stream_key, self.consumer_group, *request.message_ids)
        except redis.exceptions.ResponseError as exc:
            # The group may not exist for an orphaned stream; deletion still achieves
            # the intended permanent discard semantics for the UI.
            if "NOGROUP" not in str(exc).upper():
                raise

        discarded_count = int(self.redis.xdel(stream_key, *request.message_ids))
        self._cleanup_empty_stream(stream_key)
        logger.info(
            "Discarded DLQ messages",
            extra={
                "stream_key": stream_key,
                "discarded_count": discarded_count,
            },
        )
        return DLQDiscardResponse(discarded_count=discarded_count)

    def discard_queue(self, request: DLQQueueIdentifier) -> DLQDiscardResponse:
        """Discard an entire DLQ queue by deleting the stream key."""
        stream_key = self._stream_key(
            request.source_id,
            request.table_name,
            request.destination_id,
        )
        discarded_count = self._safe_xlen(stream_key)
        self.redis.delete(stream_key)
        logger.info(
            "Discarded DLQ queue",
            extra={"stream_key": stream_key, "discarded_count": discarded_count},
        )
        return DLQDiscardResponse(discarded_count=discarded_count)

    def discard_pipeline(self, pipeline_id: int) -> DLQPipelineDiscardResponse:
        """Discard all DLQ queues for a pipeline's source."""
        pipeline = self._get_pipeline(pipeline_id)
        if pipeline.source_id is None:
            raise ValidationError(
                message="Pipeline does not have a source_id",
                details={"field": "pipeline_id", "pipeline_id": pipeline_id},
            )

        discarded_count = 0
        queues_cleared = 0
        for stream_key in self._scan_queue_keys():
            parsed = self._parse_stream_key(stream_key)
            if not parsed:
                continue
            source_id, _table_name, _destination_id = parsed
            if source_id != pipeline.source_id:
                continue

            discarded_count += self._safe_xlen(stream_key)
            deleted = int(self.redis.delete(stream_key))
            if deleted:
                queues_cleared += 1

        logger.info(
            "Discarded DLQ for pipeline",
            extra={
                "pipeline_id": pipeline_id,
                "source_id": pipeline.source_id,
                "queues_cleared": queues_cleared,
                "discarded_count": discarded_count,
            },
        )
        return DLQPipelineDiscardResponse(
            discarded_count=discarded_count,
            queues_cleared=queues_cleared,
        )

    def _scan_queue_keys(self) -> list[str]:
        """Scan Redis for DLQ stream keys."""
        pattern = f"{self.key_prefix}:s*"
        cursor = 0
        keys: list[str] = []
        while True:
            cursor, found = self.redis.scan(cursor=cursor, match=pattern, count=100)
            keys.extend(found)
            if cursor == 0:
                break
        return keys

    def _stream_key(self, source_id: int, table_name: str, destination_id: int) -> str:
        return f"{self.key_prefix}:s{source_id}:t{table_name}:d{destination_id}"

    def _parse_stream_key(self, key: str | bytes) -> tuple[int, str, int] | None:
        key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        match = self._stream_key_pattern.match(key_str)
        if not match:
            return None
        return int(match.group(1)), match.group(2), int(match.group(3))

    def _load_pipelines_by_source(self, source_ids: set[int]) -> dict[int, Pipeline]:
        if not source_ids:
            return {}

        result = self.db.execute(
            select(Pipeline)
            .options(
                selectinload(Pipeline.source),
                selectinload(Pipeline.destinations).selectinload(
                    PipelineDestination.destination
                ),
            )
            .where(Pipeline.source_id.in_(source_ids))
        )
        pipelines = result.scalars().all()
        return {pipeline.source_id: pipeline for pipeline in pipelines if pipeline.source_id}

    def _load_destination_names(self, destination_ids: set[int]) -> dict[int, str]:
        if not destination_ids:
            return {}
        result = self.db.execute(
            select(Destination.id, Destination.name).where(
                Destination.id.in_(destination_ids)
            )
        )
        return {destination_id: name for destination_id, name in result.all()}

    def _read_boundary_message(
        self,
        stream_key: str,
        *,
        newest: bool,
    ) -> DLQMessageResponse | None:
        if newest:
            entries = self.redis.xrevrange(stream_key, max="+", min="-", count=1)
        else:
            entries = self.redis.xrange(stream_key, min="-", max="+", count=1)

        if not entries:
            return None
        entry_id, entry_data = entries[0]
        return self._parse_message(entry_id, entry_data)

    def _parse_message(
        self,
        entry_id: str | bytes,
        entry_data: dict[str, Any],
    ) -> DLQMessageResponse | None:
        raw = entry_data.get("data") if isinstance(entry_data, dict) else None
        if raw is None and isinstance(entry_data, dict):
            raw = entry_data.get(b"data")
        if raw is None:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        payload = json.loads(raw)
        cdc_record = payload.get("cdc_record") or {}
        table_sync_config = payload.get("table_sync_config") or {}
        table_name = payload.get("table_name") or cdc_record.get("table_name") or ""
        table_name_target = payload.get("table_name_target") or table_sync_config.get(
            "table_name_target"
        )
        return DLQMessageResponse(
            message_id=entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id),
            operation=cdc_record.get("operation"),
            event_timestamp=self._parse_event_timestamp(cdc_record.get("timestamp")),
            first_failed_at=self._parse_datetime(payload.get("first_failed_at")),
            retry_count=int(payload.get("retry_count") or 0),
            table_name=table_name,
            table_name_target=table_name_target,
            key=cdc_record.get("key"),
            value=cdc_record.get("value"),
            schema_payload=cdc_record.get("schema"),
            table_sync_config=table_sync_config,
        )

    def _parse_datetime(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None

    def _parse_event_timestamp(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    def _queue_search_blob(self, summary: DLQQueueSummary) -> str:
        return " ".join(
            part.lower()
            for part in [
                summary.pipeline_name or "",
                summary.source_name or "",
                summary.destination_name or "",
                summary.table_name,
                summary.table_name_target or "",
            ]
            if part
        )

    def _safe_xlen(self, stream_key: str) -> int:
        try:
            return int(self.redis.xlen(stream_key))
        except redis.exceptions.ResponseError:
            return 0

    def _cleanup_empty_stream(self, stream_key: str) -> None:
        if self._safe_xlen(stream_key) == 0:
            self.redis.delete(stream_key)

    def _get_pipeline(self, pipeline_id: int) -> Pipeline:
        result = self.db.execute(
            select(Pipeline)
            .options(selectinload(Pipeline.source))
            .where(Pipeline.id == pipeline_id)
        )
        pipeline = result.scalar_one_or_none()
        if pipeline is None:
            raise EntityNotFoundError(entity_type="Pipeline", entity_id=pipeline_id)
        return pipeline
