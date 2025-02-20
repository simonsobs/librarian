"""
ORM for the 'instances' table, describing locations of files on stores.
Also includes the ORM for the 'remote_instances' table, describing
what files have instances on remote librarians that we are aware about.
"""

from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.utils import get_hash_function_from_hash

from .. import database as db
from ..settings import server_settings


class Instance(db.Base):
    """
    Represents an instance of a file on a Store. Files are unique, Instances are not;
    there may be many copies of a single 'File' on several stores.
    """

    __tablename__ = "instances"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this instance."
    path = db.Column(db.String(256), nullable=False)
    "Full path on the store."
    file_name = db.Column(db.String(256), db.ForeignKey("files.name"), nullable=False)
    "Name of the file this instance references."
    file = db.relationship(
        "File",
        back_populates="instances",
        primaryjoin="Instance.file_name == File.name",
    )
    "The file that object is an instance of."
    store_id = db.Column(db.Integer, db.ForeignKey("store_metadata.id"), nullable=False)
    "ID of the store this instance is on."
    store = db.relationship(
        "StoreMetadata", primaryjoin="Instance.store_id == StoreMetadata.id"
    )
    "The store that this object is on."
    deletion_policy = db.Column(db.Enum(DeletionPolicy), nullable=False)
    "Whether or not this file can be deleted from the store."
    created_time = db.Column(db.DateTime, nullable=False)
    "The time at which this file was placed on the store."
    available = db.Column(db.Boolean, nullable=False)
    "Whether or not this file is available on our librarian."

    calculated_checksum = db.Column(db.String, nullable=True)
    "The checksum that has been calculated for this on-disk instance"
    calculated_size = db.Column(db.Integer, nullable=True)
    "The size of the file that was calculated at the same time as the checksum"
    checksum_time = db.Column(db.DateTime, nullable=True)
    "The time at which the calculated_checksum was checked"

    @classmethod
    def new_instance(
        self,
        path: Path,
        file: "File",
        store: "StoreMetadata",
        deletion_policy: DeletionPolicy,
    ) -> "Instance":
        """
        Create a new instance object.

        Parameters
        ----------
        path : Path
            The path of the instance.
        file : File
            The file that this instance is of.
        store : StoreMetadata
            The store that this instance is on.
        deletion_policy : DeletionPolicy
            The deletion policy for this instance.

        Returns
        -------
        Instance
            The new instance.
        """

        return Instance(
            path=str(path),
            file=file,
            store=store,
            deletion_policy=deletion_policy,
            created_time=datetime.now(timezone.utc),
            available=True,
        )

    def delete(
        self,
        session: Session,
        commit: bool = True,
        force: bool = False,
        mark_unavailable: bool = False,
    ):
        """
        Delete this instance.

        Parameters
        ----------
        session : Session
            The session to use for the deletion.
        commit : bool
            Whether or not to commit the deletion.
        force : bool
            Whether or not to force the deletion (i.e. ignore DeletionPolicy)
        mark_unavailable: bool
            If true, only mark this as unavailable, don't delete the metadata
        """

        if self.deletion_policy == DeletionPolicy.ALLOWED or force:
            self.store.store_manager.delete(Path(self.path))

        if mark_unavailable:
            logger.info("Marking instance {} as unavailable", self.id)
            self.available = False
        else:
            logger.info("Deleting instance {}", self.id)
            session.delete(self)

        if commit:
            session.commit()

        return

    def calculate_checksum(
        self,
        session: Session,
        commit: bool = True,
    ) -> tuple[str, int]:
        """
        Calculates the checksum of the instance on disk. It will use the stored checksum
        in the table instead if it has not yet timed out and has been recently calculated.

        Parameters
        ----------
        session: Session
            The database session to use; this can be committed to if commit=True below.
            Session must be active as we make sub-queries for file.
        commit: bool = True
            Whether to commit any new changes to the database.

        Returns
        -------
        checksum: str
            The checksum, calculated or drawn from the database.
        size: int
            Size of the on-disk file in bytes.

        Raises
        ------
        FileNotFoundError
            If the file was not found on disk.
        """

        current_time = datetime.now(timezone.utc)

        if self.checksum_time is not None and self.calculated_checksum is not None:
            checksum_time = self.checksum_time.astimezone(timezone.utc)
            if (current_time - checksum_time) < server_settings.checksum_timeout:
                logger.info(
                    "Returning a cached checksum from {time} for instance {id} at "
                    "{path} - {checksum}",
                    time=checksum_time,
                    id=self.id,
                    path=self.path,
                    checksum=self.calculated_checksum,
                )
                return self.calculated_checksum, self.calculated_size

        # We must calculate the checksum fresh.
        hash_function = get_hash_function_from_hash(self.file.checksum)
        path_info = self.store.store_manager.path_info(self.path, hash_function)

        logger.info(
            "Calculated a fresh checksum at {time} for instance {id} at {path} - {checksum}",
            time=current_time,
            id=self.id,
            path=self.path,
            checksum=self.calculated_checksum,
        )

        self.calculated_checksum = path_info.checksum
        self.calculated_size = path_info.size
        self.checksum_time = current_time

        if commit:
            session.commit()

        return self.calculated_checksum, self.calculated_size


class RemoteInstance(db.Base):
    """
    Remote instances of files, i.e. instances of files on remote librarians
    that we know about.
    """

    __tablename__ = "remote_instances"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
    "The unique ID of this instance."
    file_name = db.Column(db.String(256), db.ForeignKey("files.name"), nullable=True)
    "Name of the file this instance references; note this is NOT a foreign key"
    file = db.relationship(
        "File",
        back_populates="remote_instances",
        primaryjoin="RemoteInstance.file_name == File.name",
    )
    "The file that object is an instance of."
    store_id = db.Column(db.Integer, nullable=False)
    "The store ID on the remote librarian."
    librarian_id = db.Column(db.Integer, db.ForeignKey("librarians.id"), nullable=False)
    "ID of the librarian this instance is on."
    librarian = db.relationship(
        "Librarian", primaryjoin="RemoteInstance.librarian_id == Librarian.id"
    )
    "The librarian that this object is on."
    copy_time = db.Column(db.DateTime, nullable=False)
    "The time at which this file was confirmed as being fully copied to the remote librarian."
    sender = db.Column(db.String(256), nullable=False)
    "The name of the librarian that sent this file to the remote librarian."

    @classmethod
    def new_instance(
        self, file: "File", store_id: int, librarian: "Librarian"
    ) -> "RemoteInstance":
        """
        Create a new remote instance object for a clone that was
        created by us.

        Parameters
        ----------
        file : File
            The file that this instance is of.
        store_id : int
            The store ID on the remote librarian.
        librarian : Librarian
            The librarian that this instance is on.

        Returns
        -------
        RemoteInstance
            The new instance.
        """

        return RemoteInstance(
            file=file,
            store_id=store_id,
            librarian_id=librarian.id,
            librarian=librarian,
            copy_time=datetime.now(timezone.utc),
            sender=server_settings.name,
        )
