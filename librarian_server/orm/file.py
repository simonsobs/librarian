"""
ORM for 'files' table. These are unique items that have been entered into
the database. They contain, notably, a unique filename (that may actually
be a path, e.g. abcd/efgh/ijkl.txt).
"""

from .. import database as db

from pathlib import Path
from datetime import datetime

from .instance import Instance


class File(db.Base):
    """
    A file object referring to a unique instance of something entered
    into the librarian system. These are almost always the Simons Observatory
    books.

    We have removed the concept of 'observations' from the librarian relative
    to HERA because they are not as relevant to SO due to its many-instrument
    nature. The librarian is simply used for moving data around. Cataloguing is
    another piece of software's job.

    Note that filenames are unique. You should create a file with new_file
    that performs some checking against this.
    """

    __tablename__ = "files"

    # NOTE: SQLite does not allow autoincrement PKs that are BigIntegers.
    name = db.Column(db.String(256), primary_key=True, unique=True)
    "Unique filename of this file."
    create_time = db.Column(db.DateTime)
    "The time at which the file was commited to the first store. That did not necessarily happen on this librarian!"
    size = db.Column(db.BigInteger)
    "The size of the file in bytes."
    checksum = db.Column(db.String(256))
    "The checksum of the file. This is a MD5 hash of the file contents."

    uploader = db.Column(db.String(256))
    "The name of the initial uploader of this file. That did not necessarily happen on this librarian!"
    source = db.Column(db.String(256))
    "The source of this file. Could be same as uploader, but could also be another librarian."
    instances = db.relationship("Instance", back_populates="file")

    @classmethod
    def file_exists(self, filename: Path) -> bool:
        """
        Checks whether the file exists already in the database.

        Parameters
        ----------
        filename : Path
            The potential filename in the database.

        Returns
        -------
        bool
            True if it exists already.
        """

        existing_file = db.query(File, name=str(filename)).first()

        return existing_file is not None

    @classmethod
    def new_file(
        self, filename: Path, size: int, checksum: str, uploader: str, source: str
    ) -> "File":
        """
        Create a new file object.

        Parameters
        ----------
        filename : Path
            The filename of the file.
        size : int
            The size of the file in bytes.
        checksum : str
            The checksum of the file. This is a MD5 hash of the file contents.
        uploader : str
            The name of the initial uploader of this file. That did not necessarily happen on this librarian!
        source : str
            The source of this file. Could be same as uploader, but could also be another librarian.

        Returns
        -------
        File
            The new file object.
        """

        if self.file_exists(filename):
            raise FileExistsError(f"File {filename} already exists in database.")

        return File(
            name=str(filename),
            create_time=datetime.utcnow(),
            size=size,
            checksum=checksum,
            uploader=uploader,
            source=source,
        )