"""
Kafka source runner.
"""

import json
import logging
import time
from threading import Event

from config.config import get_config
from core.kafka_config import build_kafka_client_config
from core.kafka_schema_tracker import KafkaSchemaTracker
from core.models import Source
from core.record_router import RecordRouter
from core.runtime_metrics import observe, set_gauge
from destinations.base import CDCRecord
from sources.runner_base import BaseSourceRunner

logger = logging.getLogger(__name__)


class KafkaSourceRunner(BaseSourceRunner):
    """Consume Kafka topics into normalized CDC records."""

    def __init__(self, source: Source):
        self._source = source
        self._consumer = None
        self._schema_tracker = KafkaSchemaTracker(source.id)

    def _consumer_config(self) -> dict:
        return build_kafka_client_config(
            self._source.config,
            client_type="consumer",
            group_id=self._source.config.get("group_id")
            or f"rosetta-kafka-source-{self._source.id}",
        )

    def validate(self, pipeline_name: str, table_include_list: list[str]) -> None:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(
            build_kafka_client_config(self._source.config, client_type="admin")
        )
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
            poll_started = time.perf_counter()
            records_by_table: dict[str, list[CDCRecord]] = {}
            polled_messages = 0
            for _ in range(max_batch):
                msg = self._consumer.poll(1.0)
                if msg is None:
                    break
                polled_messages += 1
                if msg.error():
                    raise RuntimeError(str(msg.error()))

                record = self._message_to_record(msg)
                if record is None:
                    continue
                self._schema_tracker.track_record(
                    record.table_name,
                    record.value,
                    record.key,
                )
                records_by_table.setdefault(record.table_name, []).append(record)

            observe(
                "kafka_source.poll_duration",
                (time.perf_counter() - poll_started) * 1000.0,
                unit="ms",
                source_id=str(self._source.id),
            )

            if polled_messages:
                routed_count = sum(len(batch) for batch in records_by_table.values())
                set_gauge(
                    "kafka_source.last_batch_records",
                    routed_count,
                    unit="records",
                    source_id=str(self._source.id),
                )

            if records_by_table:
                route_started = time.perf_counter()
                router.route_batches(records_by_table)
                observe(
                    "kafka_source.route_duration",
                    (time.perf_counter() - route_started) * 1000.0,
                    unit="ms",
                    source_id=str(self._source.id),
                )
                self._consumer.commit(asynchronous=False)
            elif polled_messages:
                # Advance offsets for batches that contained only tombstones/ignored records.
                self._consumer.commit(asynchronous=False)

    def stop(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:
                logger.warning("Failed to close Kafka consumer: %s", exc)
