"""
Runtime Kafka schema tracking backed by table metadata history.
"""

from __future__ import annotations

import logging

from core.kafka_schema import infer_kafka_schema, schema_hash
from core.repository import TableMetadataRepository
from core.runtime_metrics import increment_counter

logger = logging.getLogger(__name__)


class KafkaSchemaTracker:
    """Track inferred Kafka schemas and persist version changes once per shape."""

    def __init__(self, source_id: int):
        self._source_id = source_id
        self._schema_cache: dict[str, str] = {}

    def track_record(self, table_name: str, value: dict, key: dict | None = None) -> None:
        inferred_schema = infer_kafka_schema(value, key)
        if not inferred_schema:
            return

        fingerprint = schema_hash(inferred_schema)
        if self._schema_cache.get(table_name) == fingerprint:
            return

        try:
            result = TableMetadataRepository.sync_inferred_schema(
                self._source_id,
                table_name,
                inferred_schema,
            )
        except Exception as exc:
            logger.warning(
                "Failed to persist Kafka schema for source %s table %s: %s",
                self._source_id,
                table_name,
                exc,
            )
            increment_counter(
                "kafka_source.schema_update_failures",
                source_id=str(self._source_id),
                table_name=table_name,
            )
            return

        if result.get("reason") == "missing_table":
            logger.warning(
                "Kafka schema update skipped because table metadata is missing for source %s table %s",
                self._source_id,
                table_name,
            )
            return

        persisted = result.get("schema_table") or inferred_schema
        self._schema_cache[table_name] = schema_hash(persisted)

        if result.get("updated"):
            increment_counter(
                "kafka_source.schema_updates",
                source_id=str(self._source_id),
                table_name=table_name,
                change_type=str(result.get("change_type") or "UNKNOWN"),
            )
