# Destinations module
from destinations.base import BaseDestination
from destinations.snowflake import SnowflakeDestination
from destinations.postgresql import PostgreSQLDestination

__all__ = ["BaseDestination", "SnowflakeDestination", "PostgreSQLDestination"]
