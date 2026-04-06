"""
Kafka destination.
"""

import json
import logging
from typing import Any

from core.security import decrypt_value
from destinations.base import BaseDestination, CDCRecord
from core.exceptions import DestinationException

logger = logging.getLogger(__name__)


class KafkaDestination(BaseDestination):
    """Publish CDC records to Kafka topics."""

    def __init__(self, config):
        super().__init__(config)
        self._producer = None

    def _producer_config(self) -> dict[str, Any]:
        cfg = dict(self._config.config or {})
        client = {
            "bootstrap.servers": cfg.get("bootstrap_servers"),
        }
        if cfg.get("security_protocol"):
            client["security.protocol"] = cfg["security_protocol"]
        if cfg.get("sasl_mechanism"):
            client["sasl.mechanism"] = cfg["sasl_mechanism"]
        if cfg.get("sasl_username"):
            client["sasl.username"] = cfg["sasl_username"]
        if cfg.get("sasl_password"):
            client["sasl.password"] = decrypt_value(cfg["sasl_password"])
        if cfg.get("ssl_ca_location"):
            client["ssl.ca.location"] = cfg["ssl_ca_location"]
        if cfg.get("ssl_certificate_location"):
            client["ssl.certificate.location"] = cfg["ssl_certificate_location"]
        if cfg.get("ssl_key_location"):
            client["ssl.key.location"] = cfg["ssl_key_location"]
        return client

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
        written = 0
        try:
            for record in records:
                self._producer.produce(
                    topic=topic,
                    key=self._record_key(record),
                    value=self._record_value(record),
                )
                written += 1
            self._producer.flush()
            return written
        except Exception as exc:
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
                {"bootstrap.servers": self._config.config.get("bootstrap_servers")}
            )
            admin.list_topics(timeout=10)
            return True
        except Exception as exc:
            self._logger.error("Kafka destination health check failed: %s", exc)
            return False
