from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.models.archive import (
    ArchiveCallbackFailResponse,
    ArchiveCallbackRequest,
    ArchiveCallbackResponse,
)

from ..database import yield_session
from ..logger import log
from ..orm.archive import Archive, Archivist
from .auth import SecurityDepedency

router = APIRouter(prefix="/api/v2/archive")


@router.post(
    "/callback", response_model=ArchiveCallbackResponse | ArchiveCallbackFailResponse
)
def callback(
    request: ArchiveCallbackRequest,
    response: Response,
    credentials: SecurityDepedency,
    session: Session = Depends(yield_session),
):
    """
    Callback endpoint for the archivist to call when an archive is created.
    """

    log.info(
        f"Received callback from {request.archivist_name} for archive "
        f"{request.archive_id} with manifest {request.manifest_id}"
    )

    # The archivist authenticates with the same credentials that we use to
    # call it; anything else is not the archivist.
    archivist = Archivist.check_archivist(
        name=request.archivist_name,
        username=credentials.username,
        password=credentials.password,
        session=session,
    )

    if archivist is None:
        log.warning(
            f"Rejected callback claiming to be from archivist {request.archivist_name}."
        )
        response.status_code = status.HTTP_401_UNAUTHORIZED
        response.headers["WWW-Authenticate"] = "Basic"
        return ArchiveCallbackFailResponse(
            success=False,
            reason="Unknown archivist, or incorrect username or password.",
        )

    # Check if the archive already exists
    archive_entry = session.execute(
        select(Archive).where(Archive.archive_id == request.archive_id)
    ).scalar_one_or_none()

    if not archive_entry:
        log.warning(f"Archive {request.archive_id} does not exist.")
        response.status_code = status.HTTP_404_NOT_FOUND
        return ArchiveCallbackFailResponse(
            success=False,
            reason=f"Archive {request.archive_id} does not exist.",
        )

    existing_path = archive_entry.archive_path

    if existing_path is not None and existing_path != request.archive_path:
        log.warning(
            f"Archive {request.archive_id} is already "
            f"archived at {existing_path}, but callback reports {request.archive_path}."
        )
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return ArchiveCallbackFailResponse(
            success=False,
            reason=(
                f"Archive {request.archive_id} is already archived at {existing_path}. "
                "Check the background task ordering."
            ),
        )

    archive_entry.archive_path = request.archive_path
    session.add(archive_entry)
    session.commit()

    log.info(
        f"Successfully updated archive entry for {request.archive_id} with manifest {request.manifest_id}."
    )
    return ArchiveCallbackResponse(
        success=True,
        message=f"Successfully updated archive entry for {request.archive_id} with manifest {request.manifest_id}.",
    )
