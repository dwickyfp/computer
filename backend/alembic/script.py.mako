"""
Alembic script for migration.
"""

from alembic import context

# This is the Alembic Config object
from logging.config import fileConfig

# Interpret the config file for logging
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)
