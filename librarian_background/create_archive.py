"""
Task that archives data to the archivist.
"""

import datetime
import json
import uuid

import requests
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hera_librarian.models.archive import (
    ArchiveManifestRequest,
    ArchiveManifestResponse,
    ManifestEntry,
)
from librarian_server.database import get_session
from librarian_server.orm import Archive, File, Instance

from .task import Task


class CreateArchive(Task):
    """
    A background task that send archiving requests to archivist.
    """

    librarian_name: str
    "The name of the librarian to archive files from."
    archive_name: str
    "The name of the archive to create."
    archivist_url: str
    "The URL to the archivist that will archive the files."
    age_in_days: int
    "Age in days of the files to archive."
    filesize_per_run: int = 1024
    "The total filesize of the files to archive in any one run."

    def create_manifest_request(
        self, files: list[File], manifest_id: str
    ) -> ArchiveManifestRequest:
        """
        Create the manifest request to send to the archivist.
        """

        manifest_files = []
        for file in files:
            instance = file.instances[0]

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
            archive_name=self.archive_name,
            archive_files=manifest_files,
        )

    def get_files(self, session: Session) -> list[File]:
        """
        Get the files that are candidates for archiving: those older than
        age_in_days that do not already have an entry in the archives table,
        oldest first, up to a cumulative size of filesize_per_run bytes.
        """

        cutoff = datetime.datetime.now() - datetime.timedelta(days=self.age_in_days)

        # Running total of the size of each candidate file and everything older
        # than it; a window function is needed because we cannot accumulate in
        # a WHERE clause.
        candidates = (
            select(
                File.name.label("name"),
                func.sum(File.size)
                .over(order_by=(File.create_time, File.name))
                .label("running_size"),
            )
            .where(File.create_time <= cutoff)
            .where(File.size.is_not(None))
            .where(
                select(Instance.file_name)
                .where(Instance.file_name == File.name)
                .exists()
            )
            .where(
                ~select(Archive.file_name)
                .where(Archive.file_name == File.name)
                .exists()
            )
            .subquery()
        )

        query = (
            select(File)
            .join(candidates, File.name == candidates.c.name)
            .where(candidates.c.running_size <= self.filesize_per_run)
            .order_by(candidates.c.running_size)
        )

        return session.execute(query).scalars().all()

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):

        files_to_archive = self.get_files(session)

        if not files_to_archive:
            logger.info(
                f"No files to archive to {self.archivist_url} older than {self.age_in_days} days."
            )
            return

        logger.info(f"Archiving {len(files_to_archive)} files to {self.archivist_url}.")
        manifest_id = str(uuid.uuid4())
        manifest_request = self.create_manifest_request(
            files=files_to_archive, manifest_id=manifest_id
        )

        response = requests.post(
            self.archivist_url,
            json=json.loads(manifest_request.model_dump_json()),
            timeout=30,
        )

        if response.status_code != 200:
            logger.error(
                f"Failed to send manifest to {self.archivist_url}: {response.status_code} {response.text}"
            )
            return

        manifest_response = ArchiveManifestResponse.model_validate(response.json())

        if manifest_response.archive_id != manifest_id:
            logger.error(
                f"Archiving request to {self.archivist_url} returned unexpected manifest ID: {manifest_response.manifest_id} (expected {manifest_id})"
            )
            return

        logger.info(f"Manifest sent to {self.archivist_url} successfully.")
        for file in manifest_request.archive_files:
            logger.info(f"Archived file: {file.name} ({file.size} bytes)")
            archive_entry = Archive(
                file_name=file.name,
                manifest_id=manifest_id,
                archive_path=None,
            )
            session.add(archive_entry)

        session.commit()
