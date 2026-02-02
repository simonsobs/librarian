# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""Add completed_transfers table

Revision ID: 84785333a677
Revises: 38a604ac628b
Create Date: 2025-07-25 00:51:04.843683

"""
import sqlalchemy as sa

from alembic import op

revision = "84785333a677"
down_revision = "38a604ac628b"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "completed_transfers",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.String(), nullable=False),
        sa.Column("source_endpoint_id", sa.String(), nullable=False),
        sa.Column("destination_endpoint_id", sa.String(), nullable=False),
        sa.Column("start_time", sa.DateTime(), nullable=False),
        sa.Column("end_time", sa.DateTime(), nullable=False),
        sa.Column("duration_seconds", sa.Float(), nullable=False),
        sa.Column("bytes_transferred", sa.BigInteger(), nullable=False),
        sa.Column("effective_bandwidth_bps", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["id"],
            ["send_queue.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id"),
    )

    if op.get_bind().engine.dialect.has_table(op.get_bind(), "pg_roles"):
        op.execute(
            """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafanausers') THEN
                GRANT SELECT ON completed_transfers TO grafanausers;
            END IF;
        END $$;
        """
        )


def downgrade():
    op.drop_table("completed_transfers")
