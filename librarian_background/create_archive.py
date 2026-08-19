"""
Task that archives data to the archivist.
"""

import datetime
import uuid

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hera_librarian.exceptions import (
    LibrarianError,
    LibrarianHTTPError,
    LibrarianTimeoutError,
)
from hera_librarian.models.archive import (
    ArchiveManifestRequest,
    ArchiveManifestResponse,
    ManifestEntry,
)
from librarian_server.database import get_session
from librarian_server.orm import Archive, Archivist, File, FileToArchive, Instance

from .task import Task


class CreateArchive(Task):
    """
    A background task that send archiving requests to archivist.
    """

    librarian_name: str
    "The name of the librarian to archive files from."
    archivist_name: str
    "The name of the archivist to archive files to."
    age_in_days: int
    "Age in days of the files to archive."
    filesize_per_run: int = 1024
    "The total filesize of the files to archive in any one run."
    telescope: str | None = None
    "The telescope to archive files from. If None, archive files from all telescopes."

    def create_manifest_request(
        self, files: list[tuple[File, Instance]], manifest_id: str
    ) -> ArchiveManifestRequest:
        """
        Create the manifest request to send to the archivist.
        """

        manifest_files = []
        for file, instance in files:

            manifest_files.append(
                ManifestEntry(
                    name=file.name,
                    create_time=file.create_time,
                    size=file.size,
                    checksum=file.checksum,
                    uploader=file.uploader,
                    source=file.source,
                    instance_path=instance.path,
                    deletion_policy=instance.deletion_policy,
                    instance_create_time=instance.created_time,
                    instance_available=instance.available,
                    outgoing_transfer_id=-1,
                )
            )

        return ArchiveManifestRequest(
            librarian_name=self.librarian_name,
            manifest_id=manifest_id,
            archive_files=manifest_files,
        )

    def get_files(self, session: Session) -> list[tuple[File, Instance]]:
        """
        Get the files that are candidates for archiving: those older than
        age_in_days that do not already have an entry in the archives table,
        oldest first, up to a cumulative size of filesize_per_run bytes.
        """

        chosen_instance = (
            select(
                Instance.file_name.label("file_name"),
                func.min(Instance.id).label("instance_id"),
            )
            .where(Instance.available == True)
            .group_by(Instance.file_name)
            .subquery()
        )

        cutoff = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Running total of the size of each candidate file and everything older
        # than it; a window function is needed because we cannot accumulate in
        # a WHERE clause.
        candidates = (
            select(
                File.name.label("name"),
                chosen_instance.c.instance_id.label("instance_id"),
                func.sum(File.size)
                .over(order_by=(File.create_time, File.name))
                .label("running_size"),
            )
            .join(chosen_instance, chosen_instance.c.file_name == File.name)
            .where(File.create_time <= cutoff)
            .where(File.size.is_not(None))
            .where(
                ~select(FileToArchive.file_name)
                .where(FileToArchive.file_name == File.name)
                .exists()
            )
        )

        if self.telescope is not None:
            candidates = candidates.where(
                File.name.startswith(self.telescope, autoescape=True)
            )

        candidates = candidates.subquery()

        query = (
            select(File, Instance)
            .join(candidates, File.name == candidates.c.name)
            .join(Instance, Instance.id == candidates.c.instance_id)
            .where(candidates.c.running_size <= self.filesize_per_run)
            .order_by(candidates.c.running_size)
        )

        return session.execute(query).all()

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):

        archivist: Archivist | None = (
            session.query(Archivist).filter_by(name=self.archivist_name).first()
        )

        if archivist is None:
            logger.error(
                f"Archivist {self.archivist_name} does not exist within the database. Cancelling job."
            )
            return
        files_to_archive = self.get_files(session)

        if not files_to_archive:
            logger.info(
                f"No files to archive to {self.archivist_name} older than {self.age_in_days} days."
            )
            return

        logger.info(
            f"Archiving {len(files_to_archive)} files to {self.archivist_name}."
        )
        manifest_id = str(uuid.uuid4())
        manifest_request = self.create_manifest_request(
            files=files_to_archive, manifest_id=manifest_id
        )
        client = archivist.client()

        try:
            manifest_response: ArchiveManifestResponse = client.post(
                endpoint="/api/v1/archive",
                request=manifest_request,
                response=ArchiveManifestResponse,
            )
        except (LibrarianError, LibrarianHTTPError, LibrarianTimeoutError) as e:
            logger.error(
                f"Failed to send manifest to archivist {self.archivist_name}: {e}",
                name=archivist.name,
                e=e,
            )
            return

        if manifest_response.manifest_id != manifest_id:
            logger.error(
                f"Archiving request to {self.archivist_name} returned unexpected manifest ID: {manifest_response.manifest_id} (expected {manifest_id})"
            )
            return

        archive_entry = Archive(
            manifest_id=manifest_id,
            archive_id=manifest_response.archive_id,
            archive_path=None,
        )
        session.add(archive_entry)
        logger.info(f"Manifest sent to {self.archivist_name } successfully.")
        for file in manifest_request.archive_files:
            logger.info(f"Archived file: {file.name} ({file.size} bytes)")
            archive_entry.files.append(FileToArchive(file_name=file.name))

        session.commit()
