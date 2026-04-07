import builtins

import pytest

from app.infrastructure.kafka import (
    KAFKA_DEPENDENCY_ERROR,
    create_admin_client,
    create_consumer,
)


def test_create_admin_client_raises_helpful_error_when_dependency_is_missing(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "confluent_kafka.admin":
            raise ImportError("No module named 'confluent_kafka'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError) as exc_info:
        create_admin_client({"bootstrap.servers": "localhost:9092"})

    assert str(exc_info.value) == KAFKA_DEPENDENCY_ERROR


def test_create_consumer_raises_helpful_error_when_dependency_is_missing(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "confluent_kafka":
            raise ImportError("No module named 'confluent_kafka'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError) as exc_info:
        create_consumer({"bootstrap.servers": "localhost:9092"})

    assert str(exc_info.value) == KAFKA_DEPENDENCY_ERROR
