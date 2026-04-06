import sys
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
