"""
Check in and modify the states of source and destination
transfers.
"""

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.models.status import (
    LibrarianTransfersStatusRequest,
    LibrarianTransfersStatusResponse,
    LibrarianTransfersUpdateRequest,
    LibrarianTransfersUpdateResponse,
    LibrarianTransfersFailedResponse,
)

from ..orm.librarian import Librarian
from ..database import yield_session
from ..logger import log
from .auth import CallbackUserDependency

router = APIRouter(prefix="/api/v2/transfers")

@router.post("/status", response_model=LibrarianTransfersStatusResponse)
def update(
    request: LibrarianTransfersStatusRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
) -> LibrarianTransfersStatusResponse:
    """
    Checkin and request the transfer status.
    """

    log.debug(f"Recieved checkin transfer status request from {user.username}: {request}")

    librarian = (
        session.query(Librarian).filter_by(name=user.username).one_or_none()
    )

    if librarian is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return LibrarianTransfersFailedResponse(
            reason=f"Librarian {request.librarian_name} does not exist",
            suggested_remedy="Please verify that the requested librarian exists",
        )
    
    if librarian.name != request.librarian_name:
        response.status_code = status.HTTP_403_FORBIDDEN
        return LibrarianTransfersFailedResponse(
            reason="Cannot check the status of another librarian.",
            suggested_remedy="Please verify that you are the librarian making this request",
        )

    response = LibrarianTransfersStatusResponse(
        librarian_name=librarian.name,
        transfers_enabled=librarian.transfers_enabled,
    )

    log.debug(f"Responding to checkin request with: {response}.")

    return response


@router.post("/update", response_model=LibrarianTransfersUpdateResponse)
def update(
    request: LibrarianTransfersUpdateRequest,
    response: Response,
    user: CallbackUserDependency,
    session: Session = Depends(yield_session),
) -> LibrarianTransfersUpdateResponse:
    """
    Update the transfer status.
    """

    log.debug(f"Recieved update transfer status request from {user.username}: {request}")

    librarian = (
        session.query(Librarian).filter_by(name=user.username).one_or_none()
    )

    if librarian is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return LibrarianTransfersFailedResponse(
            reason=f"Librarian {request.librarian_name} does not exist",
            suggested_remedy="Please verify that the requested librarian exists",
        )
    
    if librarian.name != request.librarian_name:
        response.status_code = status.HTTP_403_FORBIDDEN
        return LibrarianTransfersFailedResponse(
            reason="Cannot change the status of another librarian.",
            suggested_remedy="Please verify that you are the librarian making this request",
        )

    librarian.transfers_enabled = request.transfers_enabled

    session.commit()

    response = LibrarianTransfersUpdateResponse(
        librarian_name=librarian.name,
        transfers_enabled=librarian.transfers_enabled,
    )

    log.debug(f"Responding to checkin request with: {response}.")

    return response
