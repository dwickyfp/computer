"""
Rosetta Chain module for inter-instance Arrow IPC streaming.

Handles authentication, ingestion, and schema management for
data streaming between Rosetta instances.
"""

from chain.auth import validate_chain_key
from chain.ingest import ChainIngestManager
from chain.schema import ChainSchemaManager

__all__ = ["validate_chain_key", "ChainIngestManager", "ChainSchemaManager"]
