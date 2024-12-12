"""
A background task that queries the corrupt files table and remedies them.
"""

from time import perf_counter

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.errors import LibrarianError, LibrarianHTTPError
from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.database import get_session
from librarian_server.orm.file import CorruptFile, File
from librarian_server.orm.librarian import Librarian

from .task import Task


class CorruptionFixer(Task):
    """
    Checks in on corrupt files in the corrupt files table and remedies them.
    """

    def on_call(self):
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session) -> bool:
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
            logger.info(
                "Attempting to fix {id} ({name})", id=corrupt.id, name=corrupt.file_name
            )

            # Step A: Check that the file is actually corrupt
            try:
                hash_function = get_hash_function_from_hash(corrupt.file.checksum)
                instance = corrupt.instance
                store = instance.store
                path_info = store.store_manager.path_info(
                    instance.path, hash_function=hash_function
                )

                if compare_checksums(corrupt.file.checksum, path_info.checksum):
                    logger.info(
                        "CorruptFile {id} stated that file {name} was corrupt in instance {inst_id} "
                        "but we just checked the checksums: {chk_a}=={chk_b} and the file is fine "
                        "or was fixed behind our back; removing CorruptFile row",
                        id=corrupt.id,
                        name=corrupt.file_name,
                        inst_id=corrupt.instance_id,
                        chk_a=corrupt.file.checksum,
                        chk_b=path_info.checksum,
                    )
                    session.delete(corrupt)
                    session.commit()
                    continue
            except FileNotFoundError:
                logger.error(
                    "Instance {} on store {} is missing, but we will continue with recovery (Instance: {})",
                    instance.path,
                    store.name,
                    instance.id,
                )

            # Ok, so the file _really is corrupt_ or it is missing

            # Remedy A: We have another local copy of the file!
            # TODO: Implement this; it is not relevant for SO.
            if len(corrupt.file.instances) > 1:
                # Uhhh there is more than one instance here, we don't know what to do.
                logger.error(
                    "File {name} has a corrupt instance {id} but there is {n} > 1 "
                    "instances of the file on this librarian; entered block that was "
                    "never completed and need manual remedy",
                    name=corrupt.file_name,
                    id=corrupt.instance_id,
                    n=len(corrupt.file.instances),
                )
                continue

            # Remedy B: the origin of this file is another librarian. Ask for a new copy.
            stmt = select(Librarian).filter_by(name=corrupt.file.source)
            result = session.execute(stmt).scalars().one_or_none()

            if result is None:
                logger.error(
                    "File {name} has one and only one corrupt instance {id} but there is no "
                    "valid librarian matching {lib} in the librarians table so cannot "
                    "request a new valid copy of the file",
                    name=corrupt.file_name,
                    id=corrupt.instance_id,
                    lib=corrupt.file.source,
                )
                continue

            # Use the librarian to ask for a new copy.
            result: Librarian
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

            # TODO: CALL PREPARE ENDPOINT

            # TODO: Deal with the fact that we would have broken remote instances..?
            corrupt.file.delete(session=session, commit=False, force=True)

            # TODO: CALL RE-SEND ENDPOINT; DO NOT COMMIT UNTIL WE HEAR BACK; NOTE THAT WE WILL
            #       HAVE DELETED THE DATA EVEN IF WE FAIL (THAT IS NON-RECOVERABLE) BUT HAVING
            #       THE ROWS SIMPLIFIES THE LOGIC ABOVE.

            corrupt.replacement_requested = True
            session.commit()
