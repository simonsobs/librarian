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
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("manifest_id", sa.String(length=256), nullable=False),
        sa.Column("archive_id", sa.String(length=256), nullable=True),
        sa.Column("archive_path", sa.String(length=256), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("manifest_id"),
    )

    op.create_table(
        "files_to_archives",
        sa.Column("archive_id", sa.Integer(), nullable=False),
        sa.Column("file_name", sa.String(length=256), nullable=False),
        sa.ForeignKeyConstraint(
            ["archive_id"],
            ["archives.id"],
        ),
        sa.ForeignKeyConstraint(
            ["file_name"],
            ["files.name"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("archive_id", "file_name"),
    )

    op.create_table(
        "archivists",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("url", sa.String(length=256), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("authenticator", sa.String(length=256), nullable=False),
        sa.Column("last_seen", sa.DateTime(), nullable=False),
        sa.Column("last_heard", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )


def downgrade():
    op.drop_table("files_to_archives")
    op.drop_table("archives")
    op.drop_table("archivists")
