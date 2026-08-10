from .. import database as db


class Archive(db.Base):
    """
    An archive entry. Each manifest entry can contain multiple files, and each file
    can be archived multiple times. This table keeps track which files are already
    archived and what is their path.
    """

    __tablename__ = "archives"

    file_name = db.Column(
        db.String(256),
        db.ForeignKey("files.name", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    "Name of the file that was archived."
    manifest_id = db.Column(db.String(256), primary_key=True, nullable=False)
    "The ID of the manifest this file was archived as part of."

    file = db.relationship(
        "File",
        primaryjoin="Archive.file_name == File.name",
    )
    "The file that this archive entry references."

    archive_path = db.Column(db.String(256), nullable=False, unique=False)
    "The path to the archive on the store."
