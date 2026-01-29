"""
Server endpoints for validating existing files within the librarian.
This can also have a 'chaining' effect, where the server will validate
remote instances too.
"""

import asyncio
from time import perf_counter

from asyncer import asyncify
from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.exceptions import (
    LibrarianError,
    LibrarianHTTPError,
    LibrarianTimeoutError,
)
from hera_librarian.models.validate import (
    FileValidationFailedResponse,
    FileValidationRequest,
    FileValidationResponse,
    FileValidationResponseItem,
)
from hera_librarian.utils import compare_checksums

from ..database import yield_session
from ..logger import log
from ..orm.file import CorruptFile, File
from ..orm.instance import Instance
from ..orm.librarian import Librarian
from ..settings import server_settings
from .auth import ReadonlyUserDependency

router = APIRouter(prefix="/api/v2/validate")


def calculate_checksum_of_local_copy(
    original_checksum: str,
    original_size: int,
    instance: Instance,
    session: Session,
):
    start = perf_counter()
    try:
        current_checksum, current_size = instance.calculate_checksum(
            session=session, commit=True
        )
        response = FileValidationResponseItem(
            librarian=server_settings.name,
            store=instance.store_id,
            instance_id=instance.id,
            original_checksum=original_checksum,
            original_size=original_size,
            current_checksum=current_checksum,
            current_size=current_size,
            computed_same_checksum=compare_checksums(
                original_checksum, current_checksum
            ),
        )
        end = perf_counter()

        log.debug(
            f"Calculated path info for {response.instance_id} / {instance.path} "
            f"({response.current_size} B) in {end - start:.2f} seconds"
        )

        return [response]
    except FileNotFoundError:
        # A mistakenly 'available' file that is not actually available.
        log.error(
            f"File {instance.path} in store {instance.store_id} marked as available but does not exist."
        )

        return []


def calculate_checksum_of_remote_copies(
    librarian,
    file_name,
):
    start = perf_counter()
    try:
        client = librarian.client()
        client.ping()
    except (LibrarianError, LibrarianHTTPError, LibrarianTimeoutError):
        log.error(f"Unable to contact downstream librarian {librarian.name}")
        return []

    try:
        responses = client.validate_file(file_name)
        end = perf_counter()

        log.debug(
            f"Validated file {file_name} with librarian {librarian.name} in {end - start:.2f} seconds."
            f"Found {len(responses)} instances."
        )

        return responses
    except (LibrarianHTTPError, LibrarianError, LibrarianTimeoutError):
        log.error(
            f"Failed to validate file {file_name} with librarian {librarian.name}"
        )
        return []


@router.post(
    "/file", response_model=FileValidationResponse | FileValidationFailedResponse
)
async def validate_file(
    request: FileValidationRequest,
    response: Response,
    user: ReadonlyUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Validate a file within the librarian.

    Possible response codes:

    200 - OK.

    Note that the response code DOES NOT indicate whether the file is valid or not.
    The response body will contain the current checksum and the current size of the file.
    It will contain the listed checksum in this librarian's metadata, and the listed size.

    It is up to you to determine whether the file is valid or not using this information.

    Note that this will be a very slow operation! We should be able to speed this up
    by awaiting the responses from other librarians before we go away and try to calculate
    our own.
    """

    log.debug(
        f"Recieved file validation request for {request.file_name} from {user.username}: {request}"
    )

    query = select(File)

    query = query.where(File.name == request.file_name)

    file = session.execute(query).scalar()

    if not file:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return FileValidationFailedResponse(
            reason="This file does not exist in the librarian.",
            suggested_remedy="Check the file name and try again.",
        )

    coroutines = []

    # Call up our neighbours and ask them FIRST.
    # But what we actually have is a list of remote instances. There might
    # be more than one per librarian! First, use the list of remote instances
    # to generate a list of librarians we need to query.
    remote_librarian_ids = set()

    for remote_instance in file.remote_instances:
        remote_librarian_ids.add(remote_instance.librarian_id)

    # Now we can query the database for the librarians we need to query.
    for librarian_id in remote_librarian_ids:
        query = select(Librarian)

        query = query.where(Librarian.id == librarian_id)

        librarian = session.execute(query).scalar()

        if not librarian:
            continue

        # Now we can query the librarian for the file.
        responses = asyncify(calculate_checksum_of_remote_copies)(
            librarian=librarian, file_name=request.file_name
        )

        coroutines.append(responses)

    # For each instance we need to calculate the path info.
    for instance in file.instances:
        if not instance.available:
            continue

        this_checksum_info = asyncify(calculate_checksum_of_local_copy)(
            original_checksum=file.checksum,
            original_size=file.size,
            instance=instance,
            session=session,
        )

        coroutines.append(this_checksum_info)

    checksum_info = await asyncio.gather(*coroutines)

    # Flatten checksum_info
    checksum_info = [item for sublist in checksum_info for item in sublist]

    for info in checksum_info:
        if info.librarian == server_settings.name:
            query = select(CorruptFile).filter(CorruptFile.file_name == file.name)
            corrupt_file = session.execute(query).scalar_one_or_none()

        if (not info.computed_same_checksum) and info.librarian == server_settings.name:
            # Add the corrupt file to the database, though check if we already have
            # it first.
            if corrupt_file is not None:
                corrupt_file.corrupt_count += 1
                session.commit()
                continue
            else:
                corrupt_file = CorruptFile.new_corrupt_file(
                    instance=session.get(Instance, info.instance_id),
                    size=info.current_size,
                    checksum=info.current_checksum,
                )
                session.add(corrupt_file)
                session.commit()

            log.error(
                "File validation failed, the checksums do not match for file "
                "{} in store {}. CorruptFile: {}",
                request.file_name,
                info.store,
                corrupt_file.id,
            )
        elif info.librarian == server_settings.name:
            # Ok, we've got a corrupt file, but our file is fine!
            # We can delete the corrupt file.
            if corrupt_file is not None:
                log.warning(
                    "File validation succeeded, the checksums match for file {} in store {} "
                    "and corrupt file {} row has been removed",
                    request.file_name,
                    info.store,
                    corrupt_file.id,
                )
                session.delete(corrupt_file)
                session.commit()

    return FileValidationResponse(checksum_info)
