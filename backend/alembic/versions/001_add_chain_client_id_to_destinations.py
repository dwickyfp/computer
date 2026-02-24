"""Add chain_client_id to destinations table

Revision ID: 001
Revises:
Create Date: 2026-02-24

Links each auto-created ROSETTA destination back to its originating
RosettaChainClient so that create/update/delete of a chain client
can be kept in sync with the corresponding destination record.
"""

from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "destinations", sa.Column("chain_client_id", sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        "fk_destinations_chain_client_id",
        "destinations",
        "rosetta_chain_clients",
        ["chain_client_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "idx_destinations_chain_client",
        "destinations",
        ["chain_client_id"],
        postgresql_where=sa.text("chain_client_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_destinations_chain_client", table_name="destinations")
    op.drop_constraint(
        "fk_destinations_chain_client_id", "destinations", type_="foreignkey"
    )
    op.drop_column("destinations", "chain_client_id")
