"""Make rosetta_chain_tables.chain_client_id nullable, drop FK

Revision ID: 003
Revises: 002
Create Date: 2026-02-25

The chain_client_id FK was causing 500 errors when a remote Rosetta instance
registered a table schema via /chain/schema.  The remote DB has no matching
rosetta_chain_clients row for the sender's local ID, so the FK constraint
fired.  The fix makes chain_client_id nullable (no FK), and routes cross-
instance registrations via source_chain_id instead.
"""

from alembic import op
import sqlalchemy as sa

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Drop FK constraint
    op.drop_constraint(
        "rosetta_chain_tables_chain_client_id_fkey",
        "rosetta_chain_tables",
        type_="foreignkey",
    )

    # 2. Drop old table-level UNIQUE(chain_client_id, table_name) constraint
    op.drop_constraint(
        "rosetta_chain_tables_chain_client_id_table_name_key",
        "rosetta_chain_tables",
        type_="unique",
    )

    # 3. Make chain_client_id nullable
    op.alter_column(
        "rosetta_chain_tables",
        "chain_client_id",
        existing_type=sa.Integer(),
        nullable=True,
    )

    # 4. Re-add FK as SET NULL (not CASCADE) so remote registrations survive
    op.create_foreign_key(
        "fk_chain_tables_chain_client_id",
        "rosetta_chain_tables",
        "rosetta_chain_clients",
        ["chain_client_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # 5. Partial unique index for local entries (chain_client_id IS NOT NULL)
    op.create_index(
        "uq_chain_tables_local",
        "rosetta_chain_tables",
        ["chain_client_id", "table_name"],
        unique=True,
        postgresql_where=sa.text("chain_client_id IS NOT NULL"),
    )

    # 6. Partial unique index for cross-instance entries (chain_client_id IS NULL)
    op.create_index(
        "uq_chain_tables_remote",
        "rosetta_chain_tables",
        ["source_chain_id", "table_name"],
        unique=True,
        postgresql_where=sa.text("chain_client_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_chain_tables_remote", table_name="rosetta_chain_tables")
    op.drop_index("uq_chain_tables_local", table_name="rosetta_chain_tables")
    op.drop_constraint(
        "fk_chain_tables_chain_client_id", "rosetta_chain_tables", type_="foreignkey"
    )
    op.alter_column(
        "rosetta_chain_tables",
        "chain_client_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.create_unique_constraint(
        "rosetta_chain_tables_chain_client_id_table_name_key",
        "rosetta_chain_tables",
        ["chain_client_id", "table_name"],
    )
    op.create_foreign_key(
        "rosetta_chain_tables_chain_client_id_fkey",
        "rosetta_chain_tables",
        "rosetta_chain_clients",
        ["chain_client_id"],
        ["id"],
        ondelete="CASCADE",
    )
