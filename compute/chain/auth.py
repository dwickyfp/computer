"""
Chain authentication — key validation is disabled.

All incoming chain connections are accepted without authentication.
"""

import logging

logger = logging.getLogger(__name__)


def validate_chain_key(x_chain_key: str = None) -> None:
    """No-op: authentication is disabled. All connections are accepted."""
    return None
