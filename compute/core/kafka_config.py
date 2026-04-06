"""
Shared Kafka client configuration helpers.
"""

from __future__ import annotations

from typing import Any

from core.security import decrypt_value


def _optional_str(config: dict[str, Any], key: str) -> str | None:
    value = config.get(key)
    if value in (None, ""):
        return None
    return str(value)


def build_kafka_client_config(
    config: dict[str, Any] | None,
    *,
    client_type: str,
    group_id: str | None = None,
) -> dict[str, Any]:
    """
    Build a normalized Kafka client config for producer/consumer/admin clients.

    The backend and compute layers both store connection parameters in the same
    config payload, but only a subset is valid for each client role.
    """
    cfg = dict(config or {})
    client_type = client_type.lower().strip()

    client: dict[str, Any] = {
        "bootstrap.servers": cfg.get("bootstrap_servers"),
    }

    if client_type == "consumer":
        client["group.id"] = group_id or _optional_str(cfg, "group_id")
        client["auto.offset.reset"] = cfg.get("auto_offset_reset", "earliest")
        client["enable.auto.commit"] = False
    elif client_type == "producer":
        client["acks"] = cfg.get("acks", "all")
        client["enable.idempotence"] = bool(cfg.get("enable_idempotence", True))
        linger_ms = cfg.get("linger_ms")
        if linger_ms is not None:
            client["linger.ms"] = int(linger_ms)
        batch_num_messages = cfg.get("batch_num_messages")
        if batch_num_messages is not None:
            client["batch.num.messages"] = int(batch_num_messages)
    elif client_type != "admin":
        raise ValueError(f"Unsupported Kafka client type: {client_type}")

    security_protocol = _optional_str(cfg, "security_protocol")
    sasl_mechanism = _optional_str(cfg, "sasl_mechanism")
    sasl_username = _optional_str(cfg, "sasl_username")
    sasl_password = _optional_str(cfg, "sasl_password")
    ssl_ca_location = _optional_str(cfg, "ssl_ca_location")
    ssl_certificate_location = _optional_str(cfg, "ssl_certificate_location")
    ssl_key_location = _optional_str(cfg, "ssl_key_location")

    if security_protocol:
        client["security.protocol"] = security_protocol
    if sasl_mechanism:
        client["sasl.mechanism"] = sasl_mechanism
    if sasl_username:
        client["sasl.username"] = sasl_username
    if sasl_password:
        client["sasl.password"] = decrypt_value(sasl_password)
    if ssl_ca_location:
        client["ssl.ca.location"] = ssl_ca_location
    if ssl_certificate_location:
        client["ssl.certificate.location"] = ssl_certificate_location
    if ssl_key_location:
        client["ssl.key.location"] = ssl_key_location

    return {key: value for key, value in client.items() if value not in (None, "")}
