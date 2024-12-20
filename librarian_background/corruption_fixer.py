"""
A background task that queries the corrupt files table and remedies them.
"""

from datetime import datetime, timedelta, timezone
from time import perf_counter

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.exceptions import LibrarianError, LibrarianHTTPError
from hera_librarian.models.corrupt import (
    CorruptionPreparationRequest,
    CorruptionPreparationResponse,
    CorruptionResendRequest,
    CorruptionResendResponse,
)
from hera_librarian.transfer import TransferStatus
from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.orm.file import CorruptFile, File
from librarian_server.orm.instance import Instance
from librarian_server.orm.librarian import Librarian
from librarian_server.orm.transfer import IncomingTransfer
from librarian_server.settings import server_settings

from .task import Task


class CorruptionFixer(Task):
    """
    Checks in on corrupt files in the corrupt files table and remedies them.
    """

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session) -> bool:
        start_time = datetime.now(timezone.utc)
        end_time = start_time + self.soft_timeout

        query_start = perf_counter()

        stmt = (
            select(CorruptFile)
            .filter(CorruptFile.replacement_requested != True)
            .with_for_update(skip_locked=True)
        )

        results = session.execute(stmt).scalars().all()

        query_end = perf_counter()

        logger.info(
            "Took {} s to query for {} corrupt files",
            query_end - query_start,
            len(results),
        )

        for corrupt in results:
            if datetime.now(timezone.utc) > end_time:
                logger.warning(
                    "Soft timeout reached for CorruptionFixer; stopping at {time}",
                    time=datetime.now(timezone.utc),
                )
                return False

            logger.info(
                "Attempting to fix {id} ({name})", id=corrupt.id, name=corrupt.file_name
            )

            # First: query the file table to see if we still have the file. We do not store
            # a foreign key in the corrupt table because we may have deleted the file and
            # failed to contact the upstream.
            stmt = select(File).filter_by(name=corrupt.file_name)
            potential_file = session.execute(stmt).scalar_one_or_none()

            stmt = select(Instance).filter_by(id=corrupt.instance_id)
            potential_instance = session.execute(stmt).scalar_one_or_none()

            # Most likely scenario here is that the file was deleted and fixed
            # outside of this loop. Try its first instance and see if it is correct
            # then we're good to go.
            if potential_instance is None and potential_file is not None:
                try:
                    potential_instance = potential_file.instances[0]
                except IndexError:
                    potential_instance = None

            # Step A: Check that the file is actually corrupt
            try:
                hash_function = get_hash_function_from_hash(potential_file.checksum)
                store = potential_instance.store
                path_info = store.store_manager.path_info(
                    potential_instance.path, hash_function=hash_function
                )

                if compare_checksums(potential_file.checksum, path_info.checksum):
                    logger.info(
                        "CorruptFile {id} stated that file {name} was corrupt in instance {inst_id} "
                        "but we just checked the checksums: {chk_a}=={chk_b} and the file is fine "
                        "or was fixed behind our back; removing CorruptFile row",
                        id=corrupt.id,
                        name=corrupt.file_name,
                        inst_id=corrupt.instance_id,
                        chk_a=potential_file.checksum,
                        chk_b=path_info.checksum,
                    )
                    session.delete(corrupt)
                    session.commit()
                    continue

                # Remedy A: We have another local copy of the file!
                # TODO: Implement this; it is not relevant for SO.
                if len(potential_file.instances) > 1:
                    # Uhhh there is more than one instance here, we don't know what to do.
                    logger.error(
                        "File {name} has a corrupt instance {id} but there is {n} > 1 "
                        "instances of the file on this librarian; entered block that was "
                        "never completed and need manual remedy",
                        name=corrupt.file_name,
                        id=corrupt.instance_id,
                        n=len(potential_file.instances),
                    )
                    continue
            except (FileNotFoundError, AttributeError):
                logger.error(
                    "Instance {} is missing, but we will continue with recovery (File: {})",
                    corrupt.instance_id,
                    corrupt.file_name,
                )

            # Ok, so the file _really is corrupt_ or it is missing and we only have one instance

            # Remedy B: the origin of this file is another librarian. Ask for a new copy.
            stmt = select(Librarian).filter_by(name=corrupt.file_source)
            result: Librarian | None = session.execute(stmt).scalar_one_or_none()

            if result is None:
                logger.error(
                    "File {name} has one and only one corrupt instance {id} but there is no "
                    "valid librarian matching {lib} in the librarians table so cannot "
                    "request a new valid copy of the file",
                    name=corrupt.file_name,
                    id=corrupt.instance_id,
                    lib=corrupt.file_source,
                )
                continue

            # Use the librarian to ask for a new copy.
            client = result.client()

            try:
                client.ping()
            except (LibrarianError, LibrarianHTTPError):
                logger.error(
                    "Librarian {lib} is unreachable at the moment, cannot restore file {name}",
                    lib=result.name,
                    name=corrupt.file_name,
                )
                continue

            prepare_request = CorruptionPreparationRequest(
                file_name=corrupt.file_name, librarian_name=server_settings.name
            )

            try:
                prepare_response: CorruptionPreparationResponse = client.post(
                    endpoint="corrupt/prepare",
                    request=prepare_request,
                    response=CorruptionPreparationResponse,
                )

                if not prepare_response.ready:
                    raise ValueError("Preparation endpoint returned False")
            except (LibrarianError, LibrarianHTTPError, ValueError) as e:
                logger.error(
                    "Librarian {lib} contact during preparation for corruption fix to restore "
                    "{name} did not succeed: {e}",
                    lib=result.name,
                    name=corrupt.file_name,
                    e=e,
                )
                continue

            # This also deletes remote instances which will need to be repaired. However
            # it is unlikely that we will be in that situation. Unfortunately we _must_ commit
            # this as the files table must be accessed from a different table.
            if potential_file is not None:
                potential_file.delete(session=session, commit=True, force=True)

            resend_request = CorruptionResendRequest(
                file_name=corrupt.file_name,
                librarian_name=server_settings.name,
            )

            try:
                resend_response: CorruptionResendResponse = client.post(
                    "corrupt/resend",
                    request=resend_request,
                    response=CorruptionResendResponse,
                )

                if not resend_response.success:
                    raise ValueError("Failure during resend")
            except (LibrarianError, LibrarianHTTPError):
                logger.error(
                    "Failed during the resend request flow for librarian {lib}, "
                    "corrupt {id} for file {name} with {e}; we have deleted data and rows",
                    lib=result.name,
                    id=corrupt.id,
                    name=corrupt.file_name,
                    e=e,
                )
                # Can't rollback anything here so there's no point
                continue

            corrupt.incoming_transfer_id = resend_response.destination_transfer_id
            corrupt.replacement_requested = True
            session.commit()

        # Now check in on files that we already requested new copies of.
        query_start = perf_counter()

        stmt = (
            select(CorruptFile)
            .filter(CorruptFile.replacement_requested == True)
            .with_for_update(skip_locked=True)
        )

        results = session.execute(stmt).scalars().all()

        query_end = perf_counter()

        logger.info(
            "Took {} s to query for {} corrupt files already in progress",
            query_end - query_start,
            len(results),
        )

        for result in results:
            stmt = select(IncomingTransfer).filter_by(id=result.incoming_transfer_id)
            transfer = session.execute(stmt).scalar_one_or_none()

            file_is_fixed = False

            if transfer.status in [TransferStatus.FAILED, TransferStatus.CANCELLED]:
                logger.warning(
                    "Transfer for corrupt file {id} ({name}) is in status {status}",
                    id=result.id,
                    name=result.file_name,
                    status=transfer.status,
                )
                # That's no good. We should check to see if we got the file anyway:
                stmt = select(File).filter_by(name=result.file_name)
                file = session.execute(stmt).scalar_one_or_none()

                if file is not None:
                    # Oh, we're good. Phew, we successfully ingested it.
                    logger.info(
                        "Though transfer is in status {status}, file {name} was successfully "
                        "ingested anyway",
                        status=transfer.status,
                        name=result.file_name,
                    )
                    file_is_fixed = True
                else:
                    # We actually need to re-download it.
                    logger.warning(
                        "Re-setting corrupt file {id} ({name}) to not having a replacement requested "
                        "as the transfer failed. It will be re-downloaded at the next invocation ",
                        id=result.id,
                        name=result.file_name,
                    )
                    result.replacement_requested = False
            elif transfer.status in [TransferStatus.COMPLETED]:
                # That's good, we got the file!
                file_is_fixed = True
            else:
                file_is_fixed = False

            if file_is_fixed:
                logger.info(
                    "Confirmed that corrupt file {id} ({name}) has been replaced with a new copy; "
                    "deleting the CorruptFile row",
                    id=result.id,
                    name=result.file_name,
                )
                session.delete(result)

            session.commit()

        return
