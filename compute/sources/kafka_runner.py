"""
Kafka source runner.
"""

import json
import logging
import time
from threading import Event

from config.config import get_config
from core.models import Source
from core.record_router import RecordRouter
from destinations.base import CDCRecord
from sources.runner_base import BaseSourceRunner

logger = logging.getLogger(__name__)


class KafkaSourceRunner(BaseSourceRunner):
    """Consume Kafka topics into normalized CDC records."""

    def __init__(self, source: Source):
        self._source = source
        self._consumer = None

    def _consumer_config(self) -> dict:
        from core.security import decrypt_value

        config = dict(self._source.config or {})
        client = {
            "bootstrap.servers": config.get("bootstrap_servers"),
            "group.id": config.get("group_id") or f"rosetta-kafka-source-{self._source.id}",
            "auto.offset.reset": config.get("auto_offset_reset", "earliest"),
            "enable.auto.commit": True,
        }
        if config.get("security_protocol"):
            client["security.protocol"] = config["security_protocol"]
        if config.get("sasl_mechanism"):
            client["sasl.mechanism"] = config["sasl_mechanism"]
        if config.get("sasl_username"):
            client["sasl.username"] = config["sasl_username"]
        if config.get("sasl_password"):
            client["sasl.password"] = decrypt_value(config["sasl_password"])
        if config.get("ssl_ca_location"):
            client["ssl.ca.location"] = config["ssl_ca_location"]
        if config.get("ssl_certificate_location"):
            client["ssl.certificate.location"] = config["ssl_certificate_location"]
        if config.get("ssl_key_location"):
            client["ssl.key.location"] = config["ssl_key_location"]
        return client

    def validate(self, pipeline_name: str, table_include_list: list[str]) -> None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": self._source.config.get("bootstrap_servers")})
        metadata = admin.list_topics(timeout=10)
        prefix = self._source.config.get("topic_prefix")
        expected_topics = {f"{prefix}.{table}" for table in table_include_list}
        missing = sorted(topic for topic in expected_topics if topic not in metadata.topics)
        if missing:
            raise ValueError(f"Kafka source topics not found: {', '.join(missing)}")

    def _topic_to_table_name(self, topic: str) -> str:
        prefix = self._source.config.get("topic_prefix", "")
        prefix_with_dot = f"{prefix}."
        if topic.startswith(prefix_with_dot):
            return topic[len(prefix_with_dot) :]
        return topic.split(".")[-1]

    def _message_format(self) -> str:
        return str(self._source.config.get("format") or "PLAIN_JSON").upper()

    def _decode_key(self, msg) -> dict:
        key_obj = json.loads(msg.key().decode("utf-8")) if msg.key() else {}
        if isinstance(key_obj, dict) and "payload" in key_obj:
            key = key_obj["payload"]
        else:
            key = key_obj
        return key if isinstance(key, dict) else {}

    def _message_to_plain_json_record(self, msg, value_obj: dict) -> CDCRecord:
        value = dict(value_obj)
        timestamp = value.pop("rosetta_timestamp", None)
        operation = value.pop("rosetta_operation", None) or "u"
        return CDCRecord(
            operation=operation,
            table_name=self._topic_to_table_name(msg.topic()),
            key=self._decode_key(msg),
            value=value,
            schema=None,
            timestamp=timestamp if timestamp is not None else int(time.time() * 1000),
        )

    def _message_to_debezium_record(self, msg, value_obj: dict) -> CDCRecord | None:
        payload = value_obj.get("payload", {})
        op = payload.get("op")
        if op == "m":
            return None

        if op in ("c", "u", "r"):
            value = payload.get("after", {})
        elif op == "d":
            value = payload.get("before", {})
        else:
            value = payload if payload else {}

        return CDCRecord(
            operation=op or "u",
            table_name=self._topic_to_table_name(msg.topic()),
            key=self._decode_key(msg),
            value=value if isinstance(value, dict) else {},
            schema=value_obj.get("schema"),
            timestamp=payload.get("ts_ms") or int(time.time() * 1000),
        )

    def _message_to_record(self, msg) -> CDCRecord | None:
        if msg is None or msg.value() is None:
            return None

        value_obj = json.loads(msg.value().decode("utf-8"))
        if self._message_format() == "DEBEZIUM_JSON":
            return self._message_to_debezium_record(msg, value_obj)
        return self._message_to_plain_json_record(msg, value_obj)

    def run(
        self,
        pipeline_name: str,
        table_include_list: list[str],
        router: RecordRouter,
        stop_event: Event,
    ) -> None:
        from confluent_kafka import Consumer

        topic_prefix = self._source.config.get("topic_prefix")
        topics = [f"{topic_prefix}.{table}" for table in table_include_list]
        self._consumer = Consumer(self._consumer_config())
        self._consumer.subscribe(topics)

        cfg = get_config()
        max_batch = cfg.pipeline.max_batch_size
        while not stop_event.is_set():
            records_by_table: dict[str, list[CDCRecord]] = {}
            for _ in range(max_batch):
                msg = self._consumer.poll(1.0)
                if msg is None:
                    break
                if msg.error():
                    raise RuntimeError(str(msg.error()))

                record = self._message_to_record(msg)
                if record is None:
                    continue
                records_by_table.setdefault(record.table_name, []).append(record)

            if records_by_table:
                router.route_batches(records_by_table)

    def stop(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:
                logger.warning("Failed to close Kafka consumer: %s", exc)
