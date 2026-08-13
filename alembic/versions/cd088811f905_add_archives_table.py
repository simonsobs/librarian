# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD License.

"""add archives table

Revision ID: cd088811f905
Revises: 84785333a677
Create Date: 2026-08-11 14:18:24.805245

"""
import sqlalchemy as sa

from alembic import op

revision = "cd088811f905"
down_revision = "84785333a677"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "archives",
        sa.Column("file_name", sa.String(length=256), nullable=False),
        sa.Column("manifest_id", sa.String(length=256), nullable=False),
        sa.Column("archive_path", sa.String(length=256), nullable=True),
        sa.ForeignKeyConstraint(
            ["file_name"],
            ["files.name"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("file_name", "manifest_id"),
    )


def downgrade():
    op.drop_table("archives")
