# Destinations module
from destinations.base import BaseDestination
from destinations.snowflake import SnowflakeDestination
from destinations.postgresql import PostgreSQLDestination
from destinations.kafka import KafkaDestination

__all__ = [
    "BaseDestination",
    "SnowflakeDestination",
    "PostgreSQLDestination",
    "KafkaDestination",
]
