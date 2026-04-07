import sys
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace

from app.domain.services.source import SourceService


def test_system_kafka_group_id_uses_source_id():
    service = SourceService(db=None)

    assert service._system_kafka_group_id(42) == "rosetta-kafka-source-42"


def test_build_kafka_admin_client_config_excludes_consumer_only_fields():
    service = SourceService(db=None)
    source = SimpleNamespace(
        config={
            "bootstrap_servers": "localhost:9092",
            "group_id": "rosetta-kafka-source-1",
            "auto_offset_reset": "earliest",
            "security_protocol": "PLAINTEXT",
        }
    )

    config = service._build_kafka_admin_client_config(source)

    assert config == {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "PLAINTEXT",
    }


def test_latest_schema_version_uses_max_history_version():
    service = SourceService(db=None)
    histories = [
        SimpleNamespace(version_schema=1),
        SimpleNamespace(version_schema=2),
    ]

    latest = service._latest_schema_version({"id": {}}, histories)

    assert latest == 2


def test_schema_snapshot_for_historical_version_uses_history_new_schema():
    service = SourceService(db=None)
    history_v1 = SimpleNamespace(
        version_schema=1,
        schema_table_old={},
        schema_table_new={"id": {"column_name": "id", "real_data_type": "BIGINT"}},
    )
    history_v2 = SimpleNamespace(
        version_schema=2,
        schema_table_old={"id": {"column_name": "id", "real_data_type": "BIGINT"}},
        schema_table_new={
            "id": {"column_name": "id", "real_data_type": "BIGINT"},
            "name": {"column_name": "name", "real_data_type": "TEXT"},
        },
    )

    schema = service._schema_snapshot_for_version(
        table_id=3,
        version=2,
        latest_version=3,
        current_schema={
            "id": {"column_name": "id", "real_data_type": "BIGINT"},
            "name": {"column_name": "name", "real_data_type": "TEXT"},
            "active": {"column_name": "active", "real_data_type": "BOOLEAN"},
        },
        histories_by_version={1: history_v1, 2: history_v2},
    )

    assert schema == history_v2.schema_table_new


def test_schema_snapshot_for_latest_version_uses_current_schema():
    service = SourceService(db=None)
    current_schema = {
        "id": {"column_name": "id", "real_data_type": "BIGINT"},
        "name": {"column_name": "name", "real_data_type": "TEXT"},
    }

    schema = service._schema_snapshot_for_version(
        table_id=3,
        version=2,
        latest_version=2,
        current_schema=current_schema,
        histories_by_version={},
    )

    assert schema == current_schema


def test_normalize_kafka_topic_name_applies_source_prefix():
    service = SourceService(db=None)
    source = SimpleNamespace(config={"topic_prefix": "salt.public"})

    table_name, full_topic_name = service._normalize_kafka_topic_name(
        source, "orders_cdc"
    )

    assert table_name == "orders_cdc"
    assert full_topic_name == "salt.public.orders_cdc"


def test_create_kafka_topic_uses_default_retention_partition_and_replica(monkeypatch):
    created_topics = []
    invalidated = []
    commits = []
    refreshed = []

    class _Future:
        def result(self):
            return None

    class _AdminClient:
        def create_topics(self, topics, operation_timeout=None):
            created_topics.extend(topics)
            assert operation_timeout == 30
            return {topics[0].topic: _Future()}

    class _TableRepo:
        def __init__(self):
            self.rows = {}

        def get_by_source_and_name(self, source_id, table_name):
            return self.rows.get((source_id, table_name))

        def create(self, source_id, table_name, schema_table):
            self.rows[(source_id, table_name)] = SimpleNamespace(
                source_id=source_id,
                table_name=table_name,
                schema_table=schema_table,
            )

        def get_by_source_id(self, source_id):
            return [
                row
                for (row_source_id, _), row in self.rows.items()
                if row_source_id == source_id
            ]

    kafka_admin_module = ModuleType("confluent_kafka.admin")

    class NewTopic:
        def __init__(self, topic, num_partitions, replication_factor, config):
            self.topic = topic
            self.num_partitions = num_partitions
            self.replication_factor = replication_factor
            self.config = config

    kafka_admin_module.NewTopic = NewTopic
    monkeypatch.setitem(sys.modules, "confluent_kafka.admin", kafka_admin_module)

    fake_source = SimpleNamespace(
        id=2,
        type="KAFKA",
        config={
            "bootstrap_servers": "localhost:9092",
            "topic_prefix": "salt.public",
        },
        total_tables=0,
    )
    fake_repo = _TableRepo()
    fake_db = SimpleNamespace(
        commit=lambda: commits.append(True),
        refresh=lambda obj: refreshed.append(obj),
    )
    service = SourceService(db=fake_db)

    monkeypatch.setattr(service, "get_source", lambda source_id: fake_source)
    monkeypatch.setattr(service, "_invalidate_source_caches", lambda source_id: invalidated.append(source_id))
    monkeypatch.setattr("app.domain.services.source.create_admin_client", lambda config: _AdminClient())
    monkeypatch.setattr("app.domain.services.source.TableMetadataRepository", lambda db: fake_repo)

    table_name = service.create_kafka_topic(2, "orders_cdc")

    assert table_name == "orders_cdc"
    assert fake_source.total_tables == 1
    assert commits == [True]
    assert refreshed == [fake_source]
    assert invalidated == [2]
    assert len(created_topics) == 1
    assert created_topics[0].topic == "salt.public.orders_cdc"
    assert created_topics[0].num_partitions == 1
    assert created_topics[0].replication_factor == 1
    assert created_topics[0].config == {"retention.ms": "43200000"}


def test_build_kafka_consumer_client_config_disables_auto_commit():
    service = SourceService(db=None)
    source = SimpleNamespace(
        config={
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
        }
    )

    config = service._build_kafka_consumer_client_config(
        source,
        group_id="rosetta-kafka-preview-1",
        auto_offset_reset="earliest",
    )

    assert config == {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "PLAINTEXT",
        "group.id": "rosetta-kafka-preview-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }


def test_collect_kafka_topic_stats_aggregates_multi_partition_watermarks(monkeypatch):
    watermark_calls = []

    class TopicPartition:
        def __init__(self, topic, partition, offset=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class FakeConsumer:
        def get_watermark_offsets(self, topic_partition, timeout=None, cached=None):
            watermark_calls.append((topic_partition.topic, topic_partition.partition))
            values = {
                ("salt.public.orders", 0): (10, 20),
                ("salt.public.orders", 1): (12, 25),
                ("salt.public.customers", 0): (0, 3),
            }
            return values[(topic_partition.topic, topic_partition.partition)]

        def close(self):
            return None

    kafka_module = ModuleType("confluent_kafka")
    kafka_module.TopicPartition = TopicPartition
    monkeypatch.setitem(sys.modules, "confluent_kafka", kafka_module)
    monkeypatch.setattr(
        "app.domain.services.source.create_consumer",
        lambda config: FakeConsumer(),
    )

    service = SourceService(db=None)
    source = SimpleNamespace(
        id=7,
        config={"bootstrap_servers": "localhost:9092"},
    )
    metadata = SimpleNamespace(
        topics={
            "salt.public.orders": SimpleNamespace(partitions={0: object(), 1: object()}),
            "salt.public.customers": SimpleNamespace(partitions={0: object()}),
        }
    )

    stats = service._collect_kafka_topic_stats(
        source,
        [
            ("orders", "salt.public.orders"),
            ("customers", "salt.public.customers"),
        ],
        metadata,
    )

    assert stats["orders"] == {
        "first_offset": 10,
        "next_offset": 25,
        "message_count": 23,
    }
    assert stats["customers"] == {
        "first_offset": 0,
        "next_offset": 3,
        "message_count": 3,
    }
    assert watermark_calls == [
        ("salt.public.orders", 0),
        ("salt.public.orders", 1),
        ("salt.public.customers", 0),
    ]


def test_get_kafka_topic_preview_uses_ephemeral_group_without_commits(monkeypatch):
    consumer_configs = []
    created_consumers = []

    class TopicPartition:
        def __init__(self, topic, partition, offset=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class FakeMessage:
        def __init__(self, partition, offset):
            self._partition = partition
            self._offset = offset

        def key(self):
            return f'{{"id":{self._offset}}}'.encode()

        def value(self):
            return f'{{"offset":{self._offset},"partition":{self._partition}}}'.encode()

        def headers(self):
            return [("source", b'"kafka"')]

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

        def timestamp(self):
            return (1, 1_775_543_646_657)

        def error(self):
            return None

    class FakeConsumer:
        def __init__(self, config):
            self.config = config
            self.current_partition = None
            self.current_offset = 0
            self.commit_calls = 0
            self.closed = False

        def get_watermark_offsets(self, topic_partition, timeout=None, cached=None):
            return {
                0: (0, 7),
                1: (0, 7),
            }[topic_partition.partition]

        def assign(self, topic_partitions):
            topic_partition = topic_partitions[0]
            self.current_partition = topic_partition.partition
            self.current_offset = topic_partition.offset

        def poll(self, timeout=None):
            if self.current_partition is None:
                return None
            if self.current_offset >= 7:
                return None

            message = FakeMessage(self.current_partition, self.current_offset)
            self.current_offset += 1
            return message

        def commit(self, asynchronous=None):
            self.commit_calls += 1

        def close(self):
            self.closed = True

    kafka_module = ModuleType("confluent_kafka")
    kafka_module.TopicPartition = TopicPartition
    monkeypatch.setitem(sys.modules, "confluent_kafka", kafka_module)

    def consumer_factory(config):
        consumer_configs.append(config)
        consumer = FakeConsumer(config)
        created_consumers.append(consumer)
        return consumer

    service = SourceService(db=None)
    fake_source = SimpleNamespace(
        id=4,
        type="KAFKA",
        config={
            "bootstrap_servers": "localhost:9092",
            "topic_prefix": "salt.public",
        },
    )

    monkeypatch.setattr(service, "get_source", lambda source_id: fake_source)
    monkeypatch.setattr(
        service,
        "_get_kafka_metadata",
        lambda source: SimpleNamespace(
            topics={
                "salt.public.orders": SimpleNamespace(
                    partitions={0: object(), 1: object()}
                )
            }
        ),
    )
    monkeypatch.setattr(
        "app.domain.services.source.create_consumer",
        consumer_factory,
    )

    page_two = service.get_kafka_topic_preview(4, "orders", page=2)
    page_one = service.get_kafka_topic_preview(4, "orders", page=1)

    assert [message.offset for message in page_two.messages] == [3, 4, 5, 6]
    assert [message.partition for message in page_two.messages] == [1, 1, 1, 1]
    assert page_two.total_messages == 14
    assert page_two.total_pages == 2
    assert page_two.messages[0].value_preview == '{"offset":3,"partition":1}'
    assert page_two.messages[0].headers == (
        '[\n'
        '  {\n'
        '    "key": "source",\n'
        '    "value": "\\"kafka\\""\n'
        '  }\n'
        ']'
    )
    assert page_one.page == 1
    assert consumer_configs[0]["enable.auto.commit"] is False
    assert consumer_configs[1]["enable.auto.commit"] is False
    assert consumer_configs[0]["group.id"] != consumer_configs[1]["group.id"]
    assert all(consumer.commit_calls == 0 for consumer in created_consumers)
    assert all(consumer.closed for consumer in created_consumers)


def test_get_source_details_for_kafka_does_not_auto_register_discovered_topics(monkeypatch):
    class DummyRedis:
        def get(self, key):
            return None

        def setex(self, key, ttl, value):
            return None

    class TopicPartition:
        def __init__(self, topic, partition, offset=None):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class FakeConsumer:
        def get_watermark_offsets(self, topic_partition, timeout=None, cached=None):
            return (0, 5)

        def close(self):
            return None

    kafka_module = ModuleType("confluent_kafka")
    kafka_module.TopicPartition = TopicPartition
    monkeypatch.setitem(sys.modules, "confluent_kafka", kafka_module)
    monkeypatch.setattr(
        "app.infrastructure.redis.RedisClient",
        SimpleNamespace(get_instance=lambda: DummyRedis()),
    )
    monkeypatch.setattr(
        "app.domain.services.source.create_consumer",
        lambda config: FakeConsumer(),
    )

    fake_source = SimpleNamespace(
        id=8,
        name="kafka-source",
        type="KAFKA",
        config={
            "bootstrap_servers": "localhost:9092",
            "topic_prefix": "salt.public",
            "group_id": "rosetta-kafka-source-8",
            "format": "PLAIN_JSON",
        },
        is_publication_enabled=False,
        is_replication_enabled=False,
        last_check_replication_publication=None,
        total_tables=0,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    fake_table_repo = SimpleNamespace(
        get_tables_with_version_count=lambda source_id: [],
        get_by_source_id=lambda source_id: [],
    )
    fake_pipeline_repo = SimpleNamespace(get_by_source_id=lambda source_id: [])

    service = SourceService(db=None)
    monkeypatch.setattr(service, "get_source", lambda source_id: fake_source)
    monkeypatch.setattr(
        service,
        "_get_kafka_metadata",
        lambda source: SimpleNamespace(
            topics={
                "salt.public.orders": SimpleNamespace(partitions={0: object()})
            }
        ),
    )
    monkeypatch.setattr(
        "app.domain.services.source.TableMetadataRepository",
        lambda db: fake_table_repo,
    )
    monkeypatch.setattr(
        "app.domain.services.source.PipelineRepository",
        lambda db: fake_pipeline_repo,
    )

    result = service.get_source_details(8)

    assert result.tables == []
    assert result.runtime["topic_count"] == 1
    assert result.runtime["metadata_status"] == "ready"
