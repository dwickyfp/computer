from types import SimpleNamespace

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
