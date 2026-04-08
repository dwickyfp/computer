import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.domain.schemas.dlq import DLQDiscardMessagesRequest, DLQQueueIdentifier
from app.domain.services.dlq import DLQService


def _entry(
    message_id: str,
    *,
    first_failed_at: str,
    table_name: str = "public.users",
    table_name_target: str = "users_target",
    timestamp: int = 1735732800000,
    retry_count: int = 0,
) -> tuple[str, dict[str, str]]:
    payload = {
        "pipeline_id": 1,
        "source_id": 1,
        "destination_id": 2,
        "table_name": table_name,
        "table_name_target": table_name_target,
        "cdc_record": {
            "operation": "u",
            "table_name": table_name,
            "key": {"id": 1},
            "value": {"id": 1, "name": "Alice"},
            "schema": {"type": "struct"},
            "timestamp": timestamp,
        },
        "table_sync_config": {
            "id": 10,
            "table_name": table_name,
            "table_name_target": table_name_target,
        },
        "retry_count": retry_count,
        "first_failed_at": first_failed_at,
    }
    return message_id, {"data": json.dumps(payload)}


def test_parse_stream_key(mock_db_session):
    service = DLQService(mock_db_session, redis_client=MagicMock())

    assert service._parse_stream_key("rosetta:dlq:s3:tpublic.users:d9") == (
        3,
        "public.users",
        9,
    )
    assert service._parse_stream_key("rosetta:other:s3:tpublic.users:d9") is None


def test_list_queues_aggregates_with_metadata(mock_db_session):
    redis_client = MagicMock()
    redis_client.scan.side_effect = [
        (
            0,
            [
                "rosetta:dlq:s1:tpublic.users:d2",
                "rosetta:dlq:s1:tpublic.orders:d3",
            ],
        )
    ]
    redis_client.xlen.side_effect = lambda key: {
        "rosetta:dlq:s1:tpublic.users:d2": 4,
        "rosetta:dlq:s1:tpublic.orders:d3": 2,
    }[key]
    redis_client.xrevrange.side_effect = lambda key, max="+", min="-", count=1: [
        _entry(
            "2-0" if "users" in key else "4-0",
            first_failed_at="2025-01-01T12:00:00+00:00"
            if "users" in key
            else "2025-01-02T12:00:00+00:00",
            table_name="public.users" if "users" in key else "public.orders",
            table_name_target="users_target" if "users" in key else "orders_target",
        )
    ]
    redis_client.xrange.side_effect = lambda key, min="-", max="+", count=1: [
        _entry(
            "1-0" if "users" in key else "3-0",
            first_failed_at="2024-12-31T12:00:00+00:00"
            if "users" in key
            else "2025-01-01T06:00:00+00:00",
            table_name="public.users" if "users" in key else "public.orders",
            table_name_target="users_target" if "users" in key else "orders_target",
        )
    ]

    service = DLQService(mock_db_session, redis_client=redis_client)
    service._load_pipelines_by_source = MagicMock(
        return_value={
            1: SimpleNamespace(
                id=7,
                name="analytics-pipeline",
                source=SimpleNamespace(name="primary-db"),
                destinations=[],
            )
        }
    )
    service._load_destination_names = MagicMock(
        return_value={2: "snowflake-a", 3: "snowflake-b"}
    )

    result = service.list_queues(search="users")

    assert result.total_queues == 1
    assert result.total_messages == 4
    assert result.total_pipelines == 1
    assert result.total_destinations == 1
    assert result.items[0].pipeline_name == "analytics-pipeline"
    assert result.items[0].destination_name == "snowflake-a"
    assert result.items[0].table_name_target == "users_target"


def test_list_messages_uses_xrevrange_with_cursor(mock_db_session):
    redis_client = MagicMock()
    redis_client.xrevrange.return_value = [
        _entry("12-0", first_failed_at="2025-01-02T12:00:00+00:00"),
        _entry("11-0", first_failed_at="2025-01-01T12:00:00+00:00"),
    ]
    redis_client.xlen.return_value = 9

    service = DLQService(mock_db_session, redis_client=redis_client)
    result = service.list_messages(
        source_id=1,
        destination_id=2,
        table_name="public.users",
        before_id="13-0",
        limit=2,
    )

    redis_client.xrevrange.assert_called_once_with(
        "rosetta:dlq:s1:tpublic.users:d2",
        max="(13-0",
        min="-",
        count=2,
    )
    assert result.total_count == 9
    assert [item.message_id for item in result.items] == ["12-0", "11-0"]
    assert result.next_before_id == "11-0"


def test_discard_messages_acknowledges_and_deletes(mock_db_session):
    redis_client = MagicMock()
    redis_client.xdel.return_value = 2
    redis_client.xlen.return_value = 1

    service = DLQService(mock_db_session, redis_client=redis_client)
    request = DLQDiscardMessagesRequest(
        source_id=1,
        destination_id=2,
        table_name="public.users",
        message_ids=["1-0", "2-0"],
    )

    result = service.discard_messages(request)

    redis_client.xack.assert_called_once_with(
        "rosetta:dlq:s1:tpublic.users:d2",
        "dlq_recovery",
        "1-0",
        "2-0",
    )
    redis_client.xdel.assert_called_once_with(
        "rosetta:dlq:s1:tpublic.users:d2",
        "1-0",
        "2-0",
    )
    assert result.discarded_count == 2


def test_discard_queue_returns_stream_count(mock_db_session):
    redis_client = MagicMock()
    redis_client.xlen.return_value = 5
    redis_client.delete.return_value = 1

    service = DLQService(mock_db_session, redis_client=redis_client)
    result = service.discard_queue(
        DLQQueueIdentifier(
            source_id=1,
            destination_id=2,
            table_name="public.users",
        )
    )

    redis_client.delete.assert_called_once_with("rosetta:dlq:s1:tpublic.users:d2")
    assert result.discarded_count == 5


def test_discard_pipeline_clears_only_matching_source(mock_db_session):
    redis_client = MagicMock()
    redis_client.scan.side_effect = [
        (
            0,
            [
                "rosetta:dlq:s9:tpublic.users:d2",
                "rosetta:dlq:s9:tpublic.orders:d3",
                "rosetta:dlq:s5:tpublic.audit:d7",
            ],
        )
    ]
    redis_client.xlen.side_effect = lambda key: {
        "rosetta:dlq:s9:tpublic.users:d2": 3,
        "rosetta:dlq:s9:tpublic.orders:d3": 6,
        "rosetta:dlq:s5:tpublic.audit:d7": 4,
    }[key]
    redis_client.delete.return_value = 1

    service = DLQService(mock_db_session, redis_client=redis_client)
    service._get_pipeline = MagicMock(
        return_value=SimpleNamespace(id=99, source_id=9)
    )

    result = service.discard_pipeline(99)

    assert result.discarded_count == 9
    assert result.queues_cleared == 2
    redis_client.delete.assert_any_call("rosetta:dlq:s9:tpublic.users:d2")
    redis_client.delete.assert_any_call("rosetta:dlq:s9:tpublic.orders:d3")
    assert redis_client.delete.call_count == 2


def test_discard_pipeline_requires_source_id(mock_db_session):
    service = DLQService(mock_db_session, redis_client=MagicMock())
    service._get_pipeline = MagicMock(return_value=SimpleNamespace(id=7, source_id=None))

    with pytest.raises(Exception) as exc_info:
        service.discard_pipeline(7)

    assert "source_id" in str(exc_info.value)
