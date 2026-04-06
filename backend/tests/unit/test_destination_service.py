from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.domain.services.destination import DestinationService


def test_build_kafka_admin_client_config_decrypts_sasl_password(monkeypatch):
    service = DestinationService(db=MagicMock())
    destination = SimpleNamespace(
        config={
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "rosetta",
            "sasl_password": "encrypted-secret",
        }
    )

    monkeypatch.setattr(
        "app.domain.services.destination.decrypt_value",
        lambda value: f"decrypted::{value}",
    )

    config = service._build_kafka_admin_client_config(destination)

    assert config == {
        "bootstrap.servers": "localhost:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "rosetta",
        "sasl.password": "decrypted::encrypted-secret",
    }


def test_get_table_list_refreshes_kafka_topics_before_returning(monkeypatch):
    service = DestinationService(db=MagicMock())
    checked_at = datetime(2026, 4, 6, 10, 30, tzinfo=timezone.utc)
    destination = SimpleNamespace(
        id=7,
        type="KAFKA",
        config={
            "bootstrap_servers": "localhost:9092",
            "topic_prefix": "rosetta.public",
        },
        list_tables=[],
        total_tables=0,
        last_table_check_at=None,
    )

    monkeypatch.setattr(service, "get_destination", lambda _: destination)

    def sync_topics(dest):
        dest.list_tables = ["orders", "users"]
        dest.total_tables = 2
        dest.last_table_check_at = checked_at
        return dest.list_tables

    monkeypatch.setattr(service, "_sync_kafka_topics", sync_topics)

    result = service.get_table_list(7)

    assert result == {
        "tables": ["orders", "users"],
        "total_tables": 2,
        "last_table_check_at": checked_at.isoformat(),
    }


def test_dispatch_table_list_task_refreshes_kafka_inline(monkeypatch):
    service = DestinationService(db=MagicMock())
    destination = SimpleNamespace(id=9, type="KAFKA", config={})
    called_with = {}

    monkeypatch.setattr(service, "get_destination", lambda _: destination)
    monkeypatch.setattr(
        service,
        "_sync_kafka_topics",
        lambda dest: called_with.setdefault("destination_id", dest.id) or [],
    )

    task_id = service.dispatch_table_list_task(9)

    assert task_id is None
    assert called_with["destination_id"] == 9
