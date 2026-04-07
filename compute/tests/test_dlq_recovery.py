"""
Regression tests for DLQ recovery ordering and stale replay protection.
"""

import os
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

try:
    import fakeredis
except ImportError:
    pytest.skip("fakeredis not installed", allow_module_level=True)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.dlq_manager import DLQManager
from core.dlq_recovery import DLQRecoveryWorker
from core.record_router import RecordRouter
from destinations.base import CDCRecord


@pytest.fixture
def fake_redis():
    server = fakeredis.FakeServer()
    return server


@pytest.fixture
def dlq_manager(fake_redis):
    manager = DLQManager(
        redis_url="redis://localhost:6379/0",
        key_prefix="test:dlq",
        max_stream_length=1000,
        consumer_group="test_recovery",
    )
    manager._redis = fakeredis.FakeRedis(server=fake_redis, decode_responses=False)
    return manager


def _make_record(name: str, timestamp: int) -> CDCRecord:
    return CDCRecord(
        operation="u",
        table_name="users",
        key={"id": 1},
        value={"id": 1, "name": name},
        timestamp=timestamp,
    )


def _make_pipeline_context(destination_id: int = 2):
    table_sync = SimpleNamespace(
        id=101,
        pipeline_destination_id=201,
        table_name="users",
        table_name_target="users",
        filter_sql=None,
        custom_sql=None,
        is_error=False,
        error_message=None,
    )
    pipeline_destination = SimpleNamespace(
        id=201,
        destination_id=destination_id,
        is_error=False,
        error_message=None,
        table_syncs=[table_sync],
    )
    pipeline = SimpleNamespace(
        id=1,
        name="test-pipeline",
        source_id=1,
        destinations=[pipeline_destination],
    )
    return pipeline, table_sync


class RecordingDestination:
    def __init__(self, destination_type: str, destination_id: int = 2):
        self._config = SimpleNamespace(
            id=destination_id,
            name="recording-destination",
            type=destination_type,
        )
        self._is_initialized = True
        self.calls: list[list[str]] = []
        self.store: dict[int, dict] = {}
        self.block_old_write = False
        self.recovery_entered = threading.Event()
        self.allow_recovery_finish = threading.Event()
        self.live_entered = threading.Event()

    @property
    def destination_id(self) -> int:
        return self._config.id

    @property
    def name(self) -> str:
        return self._config.name

    def initialize(self, force_reconnect: bool = True) -> None:
        self._is_initialized = True

    def test_connection(self) -> bool:
        return True

    def write_batch(self, records, table_sync) -> int:
        names = [record.value["name"] for record in records]
        if self.block_old_write and names == ["old"]:
            self.recovery_entered.set()
            assert self.allow_recovery_finish.wait(timeout=5)
        else:
            self.live_entered.set()

        self.calls.append(names)
        for record in records:
            self.store[record.key["id"]] = dict(record.value)
        return len(records)


class NoForceReconnectDestination(RecordingDestination):
    def __init__(self, destination_type: str, destination_id: int = 2):
        super().__init__(destination_type, destination_id=destination_id)
        self.initialize_calls = 0

    def initialize(self) -> None:
        self.initialize_calls += 1
        self._is_initialized = True


def _version_value(dlq_manager: DLQManager, destination_id: int, record: CDCRecord):
    redis_key = dlq_manager._version_key(
        destination_id,
        record.table_name,
        dlq_manager._pk_hash(record.key),
    )
    return dlq_manager._redis.get(redis_key)


@pytest.mark.parametrize("destination_type", ["POSTGRES", "KAFKA", "SNOWFLAKE"])
def test_replay_skips_stale_dlq_message_after_newer_live_write(
    dlq_manager, destination_type
):
    pipeline, table_sync = _make_pipeline_context()
    destination = RecordingDestination(destination_type)
    router = RecordRouter(
        pipeline=pipeline,
        destinations={destination.destination_id: destination},
        dlq_manager=dlq_manager,
    )
    worker = DLQRecoveryWorker(
        pipeline=pipeline,
        destinations={destination.destination_id: destination},
        dlq_manager=dlq_manager,
    )

    old_record = _make_record("old", 1000)
    new_record = _make_record("new", 2000)

    dlq_manager.enqueue(
        pipeline_id=pipeline.id,
        source_id=pipeline.source_id,
        destination_id=destination.destination_id,
        table_name=old_record.table_name,
        table_name_target=table_sync.table_name_target,
        cdc_record=old_record,
        table_sync=table_sync,
    )

    with patch("core.record_router.DataFlowRepository.increment_count"):
        router.route_records(old_record.table_name, [new_record])

    messages = dlq_manager.dequeue_batch(
        source_id=pipeline.source_id,
        table_name=old_record.table_name,
        destination_id=destination.destination_id,
    )
    assert len(messages) == 1

    with patch(
        "core.dlq_recovery.TableSyncRepository.get_by_id", return_value=table_sync
    ), patch("core.dlq_recovery.TableSyncRepository.update_error"), patch(
        "core.dlq_recovery.PipelineDestinationRepository.update_error"
    ):
        worker._replay_messages_with_ids(
            messages,
            pipeline.source_id,
            old_record.table_name,
            destination.destination_id,
        )

    assert destination.calls == [["new"]]
    assert destination.store[1]["name"] == "new"
    assert dlq_manager.has_messages(1, "users", destination.destination_id) is False
    assert _version_value(dlq_manager, destination.destination_id, new_record) == b"2000"


@pytest.mark.parametrize("destination_type", ["POSTGRES", "KAFKA", "SNOWFLAKE"])
def test_live_write_waits_for_inflight_dlq_replay_and_newer_state_wins(
    dlq_manager, destination_type
):
    pipeline, table_sync = _make_pipeline_context()
    destination = RecordingDestination(destination_type)
    destination.block_old_write = True

    router = RecordRouter(
        pipeline=pipeline,
        destinations={destination.destination_id: destination},
        dlq_manager=dlq_manager,
    )
    worker = DLQRecoveryWorker(
        pipeline=pipeline,
        destinations={destination.destination_id: destination},
        dlq_manager=dlq_manager,
    )

    old_record = _make_record("old", 1000)
    new_record = _make_record("new", 2000)

    dlq_manager.enqueue(
        pipeline_id=pipeline.id,
        source_id=pipeline.source_id,
        destination_id=destination.destination_id,
        table_name=old_record.table_name,
        table_name_target=table_sync.table_name_target,
        cdc_record=old_record,
        table_sync=table_sync,
    )
    messages = dlq_manager.dequeue_batch(
        source_id=pipeline.source_id,
        table_name=old_record.table_name,
        destination_id=destination.destination_id,
    )
    assert len(messages) == 1

    def _run_recovery():
        worker._replay_messages_with_ids(
            messages,
            pipeline.source_id,
            old_record.table_name,
            destination.destination_id,
        )

    def _run_live():
        router.route_records(new_record.table_name, [new_record])

    with patch(
        "core.dlq_recovery.TableSyncRepository.get_by_id", return_value=table_sync
    ), patch("core.dlq_recovery.TableSyncRepository.update_error"), patch(
        "core.dlq_recovery.PipelineDestinationRepository.update_error"
    ), patch("core.record_router.DataFlowRepository.increment_count"):
        recovery_thread = threading.Thread(target=_run_recovery)
        recovery_thread.start()
        assert destination.recovery_entered.wait(timeout=5)

        live_thread = threading.Thread(target=_run_live)
        live_thread.start()
        live_thread.join(timeout=0.2)

        assert live_thread.is_alive()
        assert destination.live_entered.is_set() is False

        destination.allow_recovery_finish.set()
        recovery_thread.join(timeout=5)
        live_thread.join(timeout=5)

    assert recovery_thread.is_alive() is False
    assert live_thread.is_alive() is False
    assert destination.calls == [["old"], ["new"]]
    assert destination.store[1]["name"] == "new"
    assert _version_value(dlq_manager, destination.destination_id, new_record) == b"2000"


@pytest.mark.parametrize("destination_type", ["KAFKA", "SNOWFLAKE"])
def test_replay_reuses_initialized_destination_without_force_reconnect(
    dlq_manager, destination_type
):
    pipeline, table_sync = _make_pipeline_context()
    destination = NoForceReconnectDestination(destination_type)
    worker = DLQRecoveryWorker(
        pipeline=pipeline,
        destinations={destination.destination_id: destination},
        dlq_manager=dlq_manager,
    )

    record = _make_record("old", 1000)

    dlq_manager.enqueue(
        pipeline_id=pipeline.id,
        source_id=pipeline.source_id,
        destination_id=destination.destination_id,
        table_name=record.table_name,
        table_name_target=table_sync.table_name_target,
        cdc_record=record,
        table_sync=table_sync,
    )

    messages = dlq_manager.dequeue_batch(
        source_id=pipeline.source_id,
        table_name=record.table_name,
        destination_id=destination.destination_id,
    )
    assert len(messages) == 1

    with patch(
        "core.dlq_recovery.TableSyncRepository.get_by_id", return_value=table_sync
    ), patch("core.dlq_recovery.TableSyncRepository.update_error"), patch(
        "core.dlq_recovery.PipelineDestinationRepository.update_error"
    ):
        worker._replay_messages_with_ids(
            messages,
            pipeline.source_id,
            record.table_name,
            destination.destination_id,
        )

    assert destination.initialize_calls == 0
    assert destination.calls == [["old"]]
    assert destination.store[1]["name"] == "old"
    assert (
        dlq_manager.has_messages(
            pipeline.source_id, record.table_name, destination.destination_id
        )
        is False
    )
