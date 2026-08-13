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
from ..orm.archive import Archive
from .auth import CallbackUserDependency

router = APIRouter(prefix="/api/v2/archive")


@router.post(
    "/callback", response_model=ArchiveCallbackResponse | ArchiveCallbackFailResponse
)
def callback(
    request: ArchiveCallbackRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
):
    """
    Callback endpoint for the archivist to call when an archive is created.
    """

    log.info(
        f"Received callback for archive {request.archive_name} with manifest {request.manifest_id}"
    )

    # Check if the archive already exists
    archive_entries = (
        session.execute(
            select(Archive).where(Archive.manifest_id == request.archive_id)
        )
        .scalars()
        .all()
    )

    if not archive_entries:
        log.warning(
            f"Archive {request.archive_name} with manifest {request.manifest_id} does not exist."
        )
        response.status_code = status.HTTP_404_NOT_FOUND
        return ArchiveCallbackFailResponse(
            success=False,
            reason=f"Archive {request.archive_name} with manifest {request.manifest_id} does not exist.",
        )

    # Entries for a manifest are stamped in a single transaction, so they all
    # carry the same path. A retry naming that same path is a no-op below; one
    # naming a different path would lose the location we already recorded.
    existing_path = archive_entries[0].archive_path

    if existing_path is not None and existing_path != request.archive_path:
        log.warning(
            f"Archive {request.archive_name} with manifest {request.manifest_id} is already "
            f"archived at {existing_path}, but callback reports {request.archive_path}."
        )
        response.status_code = status.HTTP_406_NOT_ACCEPTABLE
        return ArchiveCallbackFailResponse(
            success=False,
            reason=(
                f"Manifest {request.manifest_id} is already archived at {existing_path}. "
                "Check the background task ordering."
            ),
        )

    for archive_entry in archive_entries:
        archive_entry.archive_path = request.archive_path
        session.add(archive_entry)

    session.commit()

    log.info(
        f"Successfully updated archive entry for {request.archive_name} with manifest {request.manifest_id}"
    )
    return ArchiveCallbackResponse(
        success=True,
        message=f"Successfully updated archive entry for {request.archive_name} with manifest {request.manifest_id}",
    )
