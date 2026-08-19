from datetime import datetime, timezone

from hera_librarian import ArchivistClient
from hera_librarian.exceptions import LibrarianHTTPError

from .. import database as db
from ..encryption import decrypt_string, encrypt_string


class Archive(db.Base):
    """
    An archive entry. Each manifest entry can contain multiple files, and each file
    can be archived multiple times. This table keeps track which files are already
    archived and what is their path.
    """

    __tablename__ = "archives"

    id: int = db.Column(db.Integer, primary_key=True, autoincrement=True)
    "The ID of the archive entry."

    manifest_id = db.Column(db.String(256), unique=True, nullable=False)
    "The ID of the manifest this file was archived as part of."

    archive_id = db.Column(db.String(256), nullable=True, unique=False)
    "The ID of the archive in the archivist."

    archive_path = db.Column(db.String(256), nullable=True, unique=False)
    "The path to the archive on the store."

    files = db.relationship(
        "FileToArchive",
        back_populates="archive",
        cascade="all, delete-orphan",
    )


class FileToArchive(db.Base):
    """
    A file that is to be archived. This table is used to keep track of files that
    need to be archived, but have not yet been archived.
    """

    __tablename__ = "files_to_archive"

    archive_id = db.Column(
        db.Integer,
        db.ForeignKey("archives.id"),
        primary_key=True,
        nullable=False,
    )
    "The ID of the archive entry that this file is to be archived as part of."

    file_name = db.Column(
        db.String(256),
        db.ForeignKey("files.name", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    "Name of the file that is to be archived."

    file = db.relationship(
        "File",
        primaryjoin="FileToArchive.file_name == File.name",
    )

    archive = db.relationship(
        "Archive",
        back_populates="files",
    )


class Archivist(db.Base):
    """
    A librarian that we are connected to. This should be pinged every now and then
    to confirm its availability. We will then ask for a response to see if that
    librarian knows about US; they must be able to 'call us back' for
    asynchronous transfers.
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
            The authenticator so we can connect this archivist. This is passed in
            unencrypted and will be encrypted before being stored. The authenticator
            is a username and password separated by a colon.
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

    def client(self) -> ArchivistClient:
        """
        Create a client for this archivist.

        Returns
        -------
        ArchivistClient
            The client.
        """

        decrpyted_authenticator = decrypt_string(self.authenticator)

        return ArchivistClient(
            host=self.url,
            port=self.port,
            user=decrpyted_authenticator.split(":")[0],
            password=decrpyted_authenticator.split(":")[1],
        )
