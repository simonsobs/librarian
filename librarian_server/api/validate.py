"""
Server endpoints for validating existing files within the librarian.
This can also have a 'chaining' effect, where the server will validate
remote instances too.
"""

from time import perf_counter

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity
from hera_librarian.exceptions import LibrarianError, LibrarianHTTPError
from hera_librarian.models.validate import (
    FileValidationFailedResponse,
    FileValidationRequest,
    FileValidationResponse,
    FileValidationResponseItem,
)
from hera_librarian.utils import compare_checksums, get_hash_function_from_hash

from ..database import yield_session
from ..logger import log, log_to_database
from ..orm.file import File
from ..orm.librarian import Librarian
from ..settings import server_settings
from .auth import ReadonlyUserDependency

router = APIRouter(prefix="/api/v2/validate")


@router.post(
    "/file", response_model=FileValidationResponse | FileValidationFailedResponse
)
def validate_file(
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

    checksum_info = []
    # For each instance we need to calculate the path info.
    for instance in file.instances:
        if not instance.available:
            continue

        start = perf_counter()
        hash_function = get_hash_function_from_hash(file.checksum)
        path_info = instance.store.store_manager.path_info(
            instance.path, hash_function=hash_function
        )
        checksum_info.append(
            FileValidationResponseItem(
                librarian=server_settings.name,
                store=instance.store.id,
                instance_id=instance.id,
                original_checksum=file.checksum,
                original_size=file.size,
                current_checksum=path_info.checksum,
                current_size=path_info.size,
                computed_same_checksum=compare_checksums(
                    file.checksum, path_info.checksum
                ),
            )
        )
        end = perf_counter()

        log.debug(
            f"Calculated path info for {instance.id} ({path_info.size} B) "
            f"in {end - start:.2f} seconds."
        )

        if not compare_checksums(file.checksum, path_info.checksum):
            log_to_database(
                severity=ErrorSeverity.CRITICAL,
                category=ErrorCategory.DATA_INTEGRITY,
                message=(
                    "File validation failed The checksums do not match for "
                    f"file {file.name} in store {instance.store.id}."
                ),
                session=session,
            )

    # Call up our neighbours and ask them!
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
        start = perf_counter()
        try:
            client = librarian.client()
            response = client.validate_file(file.name)
        except (LibrarianHTTPError, LibrarianError):
            log.error(
                f"Failed to validate file {file.name} with librarian {librarian.name}"
            )
            continue
        end = perf_counter()

        log.debug(
            f"Validated file {file.name} with librarian {librarian.name} in {end - start:.2f} seconds."
            f"Found {len(response)} instances."
        )

        checksum_info += response

    return FileValidationResponse(checksum_info)
