from unittest.mock import MagicMock

from app.domain.repositories.wal_monitor_repo import WALMonitorRepository
from app.domain.services.dashboard import DashboardService


def _mock_execute_all(db, rows):
    result = MagicMock()
    result.all.return_value = rows
    db.execute.return_value = result
    return result


def test_get_all_monitors_filters_to_postgres_sources():
    db = MagicMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = []
    db.execute.return_value = result

    repo = WALMonitorRepository(db)

    repo.get_all_monitors()

    statement = db.execute.call_args[0][0]
    compiled = statement.compile()

    assert "sources.type" in str(compiled)
    assert "POSTGRES" in compiled.params.values()


def test_source_health_summary_filters_to_postgres_sources():
    db = MagicMock()
    _mock_execute_all(db, [])

    service = DashboardService(db)

    summary = service.get_source_health_summary()
    statement = db.execute.call_args[0][0]
    compiled = statement.compile()

    assert summary == {"ACTIVE": 0, "IDLE": 0, "ERROR": 0, "total": 0}
    assert "sources.type" in str(compiled)
    assert "POSTGRES" in compiled.params.values()
