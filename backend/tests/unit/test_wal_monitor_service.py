from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import app.domain.services.wal_monitor as wal_monitor_module
from app.domain.services.wal_monitor import WALMonitorService


def _make_source(source_id: int, source_type: str) -> SimpleNamespace:
    return SimpleNamespace(id=source_id, name=f"source-{source_id}", type=source_type)


def test_monitor_source_sync_skips_kafka(monkeypatch, mock_db_session):
    service = WALMonitorService()

    def fail_if_called(_source):
        raise AssertionError("Kafka sources should not run WAL status checks")

    monkeypatch.setattr(service, "check_wal_status_sync", fail_if_called)

    service.monitor_source_sync(_make_source(7, "KAFKA"), mock_db_session)

    mock_db_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_monitor_all_sources_only_monitors_postgres(monkeypatch):
    service = WALMonitorService()
    monitored_source_ids: list[int] = []
    postgres_source = _make_source(1, "POSTGRES")
    kafka_source = _make_source(2, "KAFKA")

    class DummySourceRepository:
        def __init__(self, db):
            self.db = db

        def get_all(self, skip=0, limit=1000):
            return [postgres_source, kafka_source]

    @contextmanager
    def fake_session_context():
        yield object()

    async def fake_monitor_source(source, db):
        monitored_source_ids.append(source.id)

    monkeypatch.setattr(wal_monitor_module, "SourceRepository", DummySourceRepository)
    monkeypatch.setattr(wal_monitor_module, "get_session_context", fake_session_context)
    monkeypatch.setattr(service, "monitor_source", fake_monitor_source)

    await service.monitor_all_sources()

    assert monitored_source_ids == [1]
