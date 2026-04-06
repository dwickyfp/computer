"""
Kafka destination.
"""

import json
import logging
import time
from typing import Any

from core.exceptions import DestinationException
from core.kafka_config import build_kafka_client_config
from core.runtime_metrics import observe, set_gauge
from config.config import get_config
from destinations.base import BaseDestination, CDCRecord

logger = logging.getLogger(__name__)


class KafkaDestination(BaseDestination):
    """Publish CDC records to Kafka topics."""

    def __init__(self, config):
        super().__init__(config)
        self._producer = None

    def _producer_config(self) -> dict[str, Any]:
        return build_kafka_client_config(self._config.config, client_type="producer")

    def initialize(self) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(self._producer_config())
        self._is_initialized = True

    def _topic_name(self, table_name: str) -> str:
        prefix = self._config.config.get("topic_prefix")
        return f"{prefix}.{table_name}"

    def _record_key(self, record: CDCRecord) -> bytes | None:
        if not record.key:
            return None
        return json.dumps(record.key, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )

    def _message_format(self) -> str:
        return str(self._config.config.get("format") or "PLAIN_JSON").upper()

    def _record_value(self, record: CDCRecord) -> bytes:
        if self._message_format() == "DEBEZIUM_JSON":
            before = record.value if record.is_delete else None
            after = None if record.is_delete else record.value
            payload = {
                "before": before,
                "after": after,
                "op": record.operation,
                "ts_ms": record.timestamp,
            }
            envelope = {
                "schema": record.schema,
                "payload": payload,
            }
            return json.dumps(envelope, separators=(",", ":")).encode("utf-8")

        value = dict(record.value or {})
        value["rosetta_timestamp"] = record.timestamp
        value["rosetta_operation"] = record.operation
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def write_batch(self, records: list[CDCRecord], table_sync) -> int:
        if not records:
            return 0
        if not self._producer:
            self.initialize()

        topic = self._topic_name(table_sync.table_name_target or records[0].table_name)
        delivered = 0
        delivery_errors: list[str] = []
        flush_timeout = get_config().runtime.kafka_flush_timeout_seconds

        def _delivery_callback(err, msg) -> None:
            nonlocal delivered
            if err is not None:
                delivery_errors.append(str(err))
                return
            delivered += 1

        try:
            started = time.perf_counter()
            for record in records:
                self._producer.produce(
                    topic=topic,
                    key=self._record_key(record),
                    value=self._record_value(record),
                    on_delivery=_delivery_callback,
                )
                self._producer.poll(0)

            remaining = self._producer.flush(flush_timeout)
            observe(
                "kafka_destination.write_duration",
                (time.perf_counter() - started) * 1000.0,
                unit="ms",
                destination_id=str(self._config.id),
            )
            set_gauge(
                "kafka_destination.last_batch_records",
                delivered,
                unit="records",
                destination_id=str(self._config.id),
            )
            if delivery_errors:
                raise DestinationException(
                    f"Failed to write to Kafka topic {topic}: {delivery_errors[0]}",
                    {"destination_id": self._config.id, "topic": topic},
                )
            if remaining:
                raise DestinationException(
                    f"Failed to flush all Kafka messages for topic {topic}: {remaining} message(s) still pending",
                    {"destination_id": self._config.id, "topic": topic},
                )
            if delivered != len(records):
                raise DestinationException(
                    f"Kafka topic {topic} acknowledged {delivered} of {len(records)} message(s)",
                    {"destination_id": self._config.id, "topic": topic},
                )
            return delivered
        except Exception as exc:
            if isinstance(exc, DestinationException):
                raise
            raise DestinationException(
                f"Failed to write to Kafka topic {topic}: {exc}",
                {"destination_id": self._config.id, "topic": topic},
            ) from exc

    def create_table_if_not_exists(self, table_name: str, schema: dict[str, Any]) -> bool:
        return False

    def close(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush()
            except Exception as exc:
                logger.warning("Failed to flush Kafka producer: %s", exc)
        self._producer = None
        self._is_initialized = False

    def test_connection(self) -> bool:
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient(
                build_kafka_client_config(self._config.config, client_type="admin")
            )
            admin.list_topics(timeout=10)
            return True
        except Exception as exc:
            self._logger.error("Kafka destination health check failed: %s", exc)
            return False
