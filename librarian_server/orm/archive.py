import secrets
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from hera_librarian import ArchivistClient
from hera_librarian.exceptions import LibrarianHTTPError

from .. import database as db
from ..encryption import decrypt_string, encrypt_string


class Archive(db.Base):
    """
    An archive held by an archivist. One archive is created per manifest that we
    send, and contains many files, linked through `files_to_archive`. The archive path is
    filled in later, when the archivist calls back to to report it.
    """

    __tablename__ = "archives"

    id: int = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "The ID of the archive entry."

    manifest_id = db.Column(db.String(256), unique=True, nullable=False)
    "The ID of the manifest we sent to the archivist to create this archive."

    archive_id = db.Column(db.String(256), nullable=True, unique=False)
    "The ID of the archive in the archivist."

    archive_path = db.Column(db.String(256), nullable=True, unique=False)
    "The path to the archive on the archivist's store. None until it calls back."

    files = db.relationship(
        "FileToArchives",
        back_populates="archive",
        cascade="all, delete-orphan",
    )
    "The files that are part of this archive."


class FileToArchives(db.Base):
    """
    A link table between archives and the files they contain. Files are
    only ever part of a single archive, and the archive they belong to is found
    through the `archive` relationship.
    """

    __tablename__ = "files_to_archives"

    archive_id = db.Column(
        db.Integer,
        db.ForeignKey("archives.id"),
        primary_key=True,
        nullable=False,
    )
    "The ID of the archive entry that this file is archived as part of."

    file_name = db.Column(
        db.String(256),
        db.ForeignKey("files.name", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    "Name of the file that is archived."

    file = db.relationship(
        "File",
        primaryjoin="FileToArchives.file_name == File.name",
    )
    "The file that this row references."

    archive = db.relationship(
        "Archive",
        back_populates="files",
    )
    "The archive that this file is part of."


class Archivist(db.Base):
    """
    An archivist that we send archiving requests to and receive callbacks from.
    """

    __tablename__ = "archivists"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "Unique ID of this archivist (relative to us)."
    name = db.Column(db.String(256), nullable=False, unique=True)
    "The name of this archivist."
    url = db.Column(db.String(256), nullable=False)
    "The URL of this archivist."
    port = db.Column(db.Integer, nullable=False)
    "The port of this archivist."
    authenticator = db.Column(db.String(256), nullable=False)
    "The authenticator so we can connect this archivist. This is encrypted."

    last_seen = db.Column(db.DateTime, nullable=False)
    "The last time we connected to and verified this archivist exists."
    last_heard = db.Column(db.DateTime, nullable=False)
    "The last time we heard from this archivist (the last time it connected to us)."

    @classmethod
    def new_archivist(
        cls,
        name: str,
        url: str,
        port: int,
        authenticator: str,
        check_connection: bool = True,
    ) -> "Archivist":
        """
        Create a new archivist object.

        Parameters
        ----------
        name : str
            The name of this archivist.
        url : str
            The URL of this archivist.
        port : int
            The port of this archivist.
        authenticator : str
            The authenticator so we can connect this archivist.
        check_connection : bool
            Whether to check the connection to this archivist before
            returning it (default: True, but turn this off for tests.)

        Returns
        -------
        Archivist
            The new archivist.

        Raises
        ------
        ValueError
            No encryption key is set!
        """

        archivist = Archivist(
            name=name,
            url=url,
            port=port,
            authenticator=encrypt_string(authenticator),
            last_seen=datetime.now(timezone.utc),
            last_heard=datetime.now(timezone.utc),
        )

        if not check_connection:
            return archivist

        # Before returning it, we should ping it to confirm it exists.

        client = archivist.client()

        try:
            client.ping()
        except LibrarianHTTPError:
            raise ValueError("Archivist does not exist or is unreachable.")

        archivist.last_seen = datetime.now(timezone.utc)

        return archivist

    @classmethod
    def check_archivist(
        cls, name: str, username: str, password: str, session: Session
    ) -> "Archivist | None":
        """
        Check that an archivist exists and that the credentials it presented
        are the ones we hold for it.

        Parameters
        ----------
        name : str
            The name of the archivist that claims to be calling.
        username : str
            The username presented by the caller.
        password : str
            The password presented by the caller.
        session : Session
            The database session to use.

        Returns
        -------
        Archivist | None
            The archivist, or None if it does not exist or the credentials
            do not match.
        """

        archivist = session.query(cls).filter_by(name=name).one_or_none()

        if archivist is None:
            return None

        stored_username, _, stored_password = decrypt_string(
            archivist.authenticator
        ).partition(":")

        if secrets.compare_digest(
            stored_username.encode(), username.encode()
        ) and secrets.compare_digest(stored_password.encode(), password.encode()):
            return archivist

        return None

    def client(self) -> ArchivistClient:
        """
        Create a client for this archivist.

        Returns
        -------
        ArchivistClient
            The client.
        """

        username, _, password = decrypt_string(self.authenticator).partition(":")

        return ArchivistClient(
            host=self.url,
            port=self.port,
            user=username,
            password=password,
        )
