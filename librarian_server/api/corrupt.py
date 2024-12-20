"""
API Endpoints for the upstream half of the corrupt files workflow.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.exceptions import LibrarianError, LibrarianHTTPError
from hera_librarian.models.corrupt import (
    CorruptionPreparationRequest,
    CorruptionPreparationResponse,
    CorruptionResendRequest,
    CorruptionResendResponse,
)
from hera_librarian.utils import compare_checksums, get_hash_function_from_hash
from librarian_server.orm.file import File
from librarian_server.orm.instance import Instance, RemoteInstance
from librarian_server.orm.librarian import Librarian

router = APIRouter(prefix="/api/v2/corrupt")

from loguru import logger

from ..database import yield_session
from .auth import CallbackUserDependency, User


def user_and_librarian_validation_flow(
    user: User, librarian_name: str, file_name: str, session: Session
) -> tuple[Librarian, File, Instance, list[RemoteInstance]]:
    """
    Figure out if this user is a librarian and that we can make file transfers
    to that librarian for this file. Also validates the file on our librarian to make
    sure it is not corrupt and is present.
    """
    user_is_librarian = user.username == librarian_name

    stmt = select(Librarian).filter_by(name=librarian_name)
    librarian = session.execute(stmt).scalars().one_or_none()

    librarian_exists = librarian is not None

    if not librarian_exists:
        logger.warning(
            "Librarian {} does not exist, cannot authenticate remedy request",
            librarian_name,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=dict(
                reason="Unauthorized",
                suggested_remedy="",
            ),
        )

    stmt = select(RemoteInstance).filter_by(
        file_name=file_name, librarian_id=librarian.id
    )
    remote_instances = session.execute(stmt).scalars().all()

    remote_instance_registered_at_destination = bool(remote_instances)

    if not (
        # remote_instance_registered_at_destination
        user_is_librarian
        and librarian_exists
    ):
        logger.debug(
            "Problem authenticating remedy request, Remote instance: {}, User is librarian: {}, Librarian exists: {}",
            remote_instance_registered_at_destination,
            user_is_librarian,
            librarian_exists,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=dict(
                reason="Unauthorized",
                suggested_remedy="",
            ),
        )

    # So at this point we know:
    # Downstream is the one asking for the new copy
    # We sent them a copy that we confirmed

    # Check our own instance of the file to make sure it's not corrupted.
    stmt = select(File).filter_by(name=file_name)
    file = session.execute(stmt).scalars().one_or_none()

    try:
        best_instance = [x for x in file.instances if x.available][0]
    except IndexError:
        raise HTTPException(
            status_code=status.HTTP_409_BAD_REQUEST,
            detail=dict(
                reason="We do not have a copy of the file you are requesting",
                suggested_remedy="Check your database; you likely did not get the file from us",
            ),
        )

    hash_function = get_hash_function_from_hash(file.checksum)
    path_info = best_instance.store.store_manager.path_info(
        best_instance.path, hash_function=hash_function
    )

    if not compare_checksums(file.checksum, path_info.checksum):
        logger.error(
            "Our copy of the file {} is corrupt, we cannot send it to {}",
            file_name,
            librarian_name,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=dict(
                reason="Our copy of the file is also corrupt",
                suggested_remedy="Wait a while, we will attempt to fix this copy",
            ),
        )
        # Brother not this shit again
        # Add to corrupt files table?
        # Extremely unlikely

    # We know we have a valid copy of the file ready to go.

    # Do we have login details for your librarian?
    login_success = True
    try:
        librarian.client().ping(require_login=True)
    except (LibrarianError, LibrarianHTTPError):
        login_success = False

    from librarian_background import background_settings

    if not (
        background_settings.consume_queue
        and background_settings.check_consumed_queue
        and librarian.transfers_enabled
        and login_success
    ):
        logger.warning(
            "Unable to transfer files to downstream librarian {}: "
            "Consume queue: {}, check consume queue: {}, transfers enabled: {}, login success: {}",
            librarian.name,
            bool(background_settings.consume_queue),
            bool(background_settings.check_consumed_queue),
            librarian.transfers_enabled,
            login_success,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=dict(
                reason="We are not able to send you files",
                suggested_remedy="Check every pre-condition for file transfers is met",
            ),
        )

    return librarian, file, best_instance, remote_instances


@router.post("/prepare")
def prepare(
    request: CorruptionPreparationRequest,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
) -> CorruptionPreparationResponse:
    """
    Prepare for a request to re-instate a downstream file. This checks:

    a) We can contact the downstream
    b) We have a valid copy of the file
    c) We have a send queue background task that will actually send the file.

    Possible response codes:

    409 - We do not have a valid copy of the file either!
        -> You are out of luck. Maybe try again later as we might restore from
           a librarian above us in the chain?
    401 - You are asking about a file that was not sent to your librarian
        -> Leave me alone!
    200 - Ready to send
        -> Success!
    """

    logger.info(
        "Recieved corruption remedy request for {} from {}",
        request.file_name,
        user.username,
    )

    user_and_librarian_validation_flow(
        user,
        librarian_name=request.librarian_name,
        file_name=request.file_name,
        session=session,
    )

    logger.info(
        "Prepared to send a new copy of {} to {}",
        request.file_name,
        request.librarian_name,
    )

    return CorruptionPreparationResponse(ready=True)


@router.post("/resend")
def resend(
    request: CorruptionResendRequest,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
) -> CorruptionResendResponse:
    """
    Actually send a new copy of a file that we know you already have! We assume that
    you deleted it before you called this endpoint, and that you called the prepare
    endpoint to make sure we're all good to go first. We will:

    a) Delete our RemoteInstance(s) for this file on your librarian
    b) Create an OutgoingTransfer and SendQueue

    This transfer will then take place asynchronously through your usual mechanisms.
    You _must_ have a recieve clone task running on your librarian otherwise you won't
    have the new file ingested.

    Possible response codes:

    409 - We don't have a valid copy of the file.
    201 - We created the transfer
        -> Success!
    """

    logger.info(
        "Recieved corruption resend request for {} from {}",
        request.file_name,
        user.username,
    )

    librarian, file, instance, remote_instances = user_and_librarian_validation_flow(
        user,
        librarian_name=request.librarian_name,
        file_name=request.file_name,
        session=session,
    )

    from librarian_background.send_clone import send_file_batch

    success = send_file_batch(files=[file], librarian=librarian, session=session)

    if success:
        logger.info(
            "Successfully created send queue item to remedy corrupt data in {}",
            request.file_name,
        )
        for ri in remote_instances:
            session.delete(ri)
        session.commit()
        return CorruptionResendResponse(
            success=bool(success),
            destination_transfer_id=success[0].destination_transfer_id,
        )
    else:
        logger.info(
            "Error creating send queue item to remedy corrupt data in {}",
            request.file_name,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=dict(
                reason="Error creating send queue item",
                suggested_remedy="Check the logs for more information",
            ),
        )
