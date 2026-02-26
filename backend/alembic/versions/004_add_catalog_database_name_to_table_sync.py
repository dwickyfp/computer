"""Add catalog_database_name to pipelines_destination_table_sync

Revision ID: 004
Revises: 003
Create Date: 2026-02-25

Adds catalog_database_name column to pipelines_destination_table_sync so that
the Rosetta Chain schema registration dialog can persist the selected remote
database name per table sync branch. This enables:
- Active-style indicator on the "Target Settings" button
- Pre-population of the database selection when reopening the dialog
"""

from alembic import op
import sqlalchemy as sa

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "pipelines_destination_table_sync",
        sa.Column(
            "catalog_database_name",
            sa.String(255),
            nullable=True,
            comment="Destination database name on the remote Rosetta Chain instance",
        ),
    )


def downgrade() -> None:
    op.drop_column("pipelines_destination_table_sync", "catalog_database_name")
