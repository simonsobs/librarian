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
from librarian_server.orm import Archive, Archivist, File, FileToArchives, Instance
from librarian_server.settings import server_settings

from .task import Task


class CreateArchive(Task):
    """
    A background task that send archiving requests to archivist.
    """

    archivist_name: str
    "The name of the archivist to archive files to."
    age_in_days: int
    "Age in days of the files to archive."
    filesize_per_run: int
    "The total filesize, in bytes, of the files to archive in any one run."
    match_query: str | None = None
    "A SQL LIKE pattern matched against the file name."

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
            librarian_name=server_settings.name,
            manifest_id=manifest_id,
            archive_files=manifest_files,
        )

    def get_files(self, session: Session) -> list[tuple[File, Instance]]:
        """
        Get the files that are candidates for archiving.

        They are older than age_in_days, have at least one available instance,
        and have not archived yet. If `match_query` then only the files whose
        name includes `match_query` are selected. The total of all files is not
        more than `filesize_per_run` in bytes.

        Returns
        -------
        list[tuple[File, Instance]]
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
                ~select(FileToArchives.file_name)
                .where(FileToArchives.file_name == File.name)
                .exists()
            )
        )

        if self.match_query is not None:
            candidates = candidates.where(File.name.like(self.match_query))

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

        # Create the DB entries for the files before sending the request. If
        # the request fails because of a problem, we do not want these files to
        # picked up again.
        archive_entry = Archive(
            manifest_id=manifest_id,
            archive_id=None,
            archive_path=None,
        )
        session.add(archive_entry)
        for file in manifest_request.archive_files:
            archive_entry.files.append(FileToArchives(file_name=file.name))

        session.commit()

        try:
            manifest_response: ArchiveManifestResponse = client.post(
                endpoint="archive",
                request=manifest_request,
                response=ArchiveManifestResponse,
            )
        except (LibrarianError, LibrarianHTTPError, LibrarianTimeoutError) as e:
            # Nothing was accepted, so release the claim and let the files be
            # picked up again by a later run.
            logger.error(
                f"Failed to send manifest to archivist {self.archivist_name}: {e}"
            )
            session.delete(archive_entry)
            session.commit()
            return

        archive_entry.archive_id = manifest_response.archive_id
        session.commit()

        if manifest_response.manifest_id != manifest_id:
            # The archivist has taken the files but disagrees about which request
            # this was. Keep the claim so we do not send them again, and leave the
            # entry to be reconciled by hand.
            logger.error(
                f"Archiving request to {self.archivist_name} returned unexpected "
                f"manifest ID: {manifest_response.manifest_id} (expected {manifest_id}). "
                f"Archive {manifest_response.archive_id} recorded; reconcile manually."
            )
            return

        logger.info(f"Manifest sent to {self.archivist_name} successfully.")
        for file in manifest_request.archive_files:
            logger.info(f"Archived file: {file.name} ({file.size} bytes)")
