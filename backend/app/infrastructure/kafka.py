"""
Kafka client helpers for the backend.
"""

from typing import Any

KAFKA_DEPENDENCY_ERROR = (
    "Kafka support requires the backend dependency 'confluent-kafka'. "
    "Run `uv sync` in /backend or install `requirements.txt` for the backend service."
)


def create_admin_client(client_config: dict[str, Any]):
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError as exc:
        raise RuntimeError(KAFKA_DEPENDENCY_ERROR) from exc

    return AdminClient(client_config)
