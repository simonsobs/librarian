# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.


"""Add librarian transfer toggling and corruption

Revision ID: 1def8c988372
Revises: 42f29c26ab0f
Create Date: 2024-11-11 15:09:12.578181

"""
import sqlalchemy as sa

from alembic import op

revision = "1def8c988372"
down_revision = "42f29c26ab0f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("librarians") as batch_op:
        batch_op.add_column(
            sa.Column("transfers_enabled", sa.Boolean(), nullable=True, default=True)
        )

    op.create_table(
        "corrupt_files",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("file_name", sa.String(), nullable=False),
        sa.Column("file_source", sa.String(), nullable=False),
        sa.Column("instance_id", sa.Integer(), nullable=False),
        sa.Column("instance_path", sa.String(), nullable=False),
        sa.Column("corrupt_time", sa.DateTime(), nullable=False),
        sa.Column("size", sa.BigInteger(), nullable=False),
        sa.Column("checksum", sa.String(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("replacement_requested", sa.Boolean(), nullable=False),
        sa.Column("incoming_transfer_id", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Check if role exists before granting SELECT permissions
    if op.get_bind().engine.dialect.has_table(op.get_bind(), "pg_roles"):
        op.execute(
            """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafanausers') THEN
                GRANT SELECT ON corrupt_files TO grafanausers;
            END IF;
        END $$;
        """
        )

    with op.batch_alter_table("outgoing_transfers") as batch_op:
        batch_op.alter_column("file_name", nullable=True)


def downgrade():
    op.drop_column("librarians", "transfers_enabled")
    op.drop_table("corrupt_files")

    with op.batch_alter_table("outgoing_transfers") as batch_op:
        batch_op.alter_column("file_name", nullable=False)
