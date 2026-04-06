from app.domain.services.source import SourceService


def test_system_kafka_group_id_uses_source_id():
    service = SourceService(db=None)

    assert service._system_kafka_group_id(42) == "rosetta-kafka-source-42"
