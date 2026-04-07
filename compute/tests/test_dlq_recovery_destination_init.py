"""
Regression tests for destination initialization in DLQ replay.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.dlq_recovery import DLQRecoveryWorker


def _make_worker() -> DLQRecoveryWorker:
    pipeline = SimpleNamespace(id=1, name="test-pipeline", source_id=1, destinations=[])
    return DLQRecoveryWorker(
        pipeline=pipeline,
        destinations={},
        dlq_manager=SimpleNamespace(),
    )


def test_replay_reuses_initialized_destination_without_force_reconnect():
    worker = _make_worker()

    class KafkaLikeDestination:
        def __init__(self):
            self._is_initialized = True
            self.name = "kafka-destination"
            self.initialize_calls = 0

        def initialize(self) -> None:
            self.initialize_calls += 1

    destination = KafkaLikeDestination()

    worker._ensure_destination_ready_for_replay(destination)

    assert destination.initialize_calls == 0


def test_replay_uses_non_forced_reconnect_when_supported():
    worker = _make_worker()

    class PostgresLikeDestination:
        def __init__(self):
            self._is_initialized = True
            self.name = "postgres-destination"
            self.calls: list[bool] = []

        def initialize(self, force_reconnect: bool = False) -> None:
            self.calls.append(force_reconnect)

    destination = PostgresLikeDestination()

    worker._ensure_destination_ready_for_replay(destination)

    assert destination.calls == [False]
