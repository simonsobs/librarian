# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""Add cached checksums

Revision ID: 38a604ac628b
Revises: 1def8c988372
Create Date: 2025-02-20 11:12:54.446089

"""
import sqlalchemy as sa

from alembic import op

revision = "38a604ac628b"
down_revision = "1def8c988372"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("instances") as batch_op:
        batch_op.add_column(
            sa.Column("calculated_checksum", sa.String(), nullable=True)
        )
        batch_op.add_column(
            sa.Column("calculated_size", sa.BigInteger(), nullable=True)
        )
        batch_op.add_column(sa.Column("checksum_time", sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column("instances", "calculated_checksum")
    op.drop_column("instances", "calculated_size")
    op.drop_column("instances", "checksum_time")
