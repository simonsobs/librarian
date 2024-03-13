"""
Administration endpoints. Used for managing the librarian server,
and handling in-place updates to the server (e.g. adding File and
Instance objects to the database, updating the database, etc. without
actually ingesting files).
"""

from pathlib import Path
from typing import Union

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.admin import (
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminRequestFailedResponse,
    AdminVerifyFileRequest,
    FileVerificationResponse,
)
from hera_librarian.utils import get_md5_from_path, get_size_from_path

from ..database import yield_session
from ..orm import File, Instance, StoreMetadata
from ..stores import StoreNames
from .auth import AdminUserDependency

router = APIRouter(prefix="/api/v2/admin")


@router.post("/add_file")
def add_file(
    request: AdminCreateFileRequest,
    user: AdminUserDependency,
    response: Response,
    session: Session = Depends(yield_session),
):
    """
    Creates a new file and instance in the database, assuming
    that a file already exists. If the file does not exist on the
    store already, we error out.
    """

    # First, get the store.
    store = (
        session.query(StoreMetadata).filter_by(name=request.store_name).one_or_none()
    )

    if store is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} does not exist.",
            suggested_remedy="Create the store first. Maybe you need to run DB migration?",
        )

    # TODO: Can't do code coverage until we add nonlocal stores.
    if store.store_type != StoreNames["local"]:  # pragma: no cover
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"Store {request.store_name} is not a local store.",
            suggested_remedy="Use a local store for this operation.",
        )

    # Check if the file exists already.
    existing_file = session.get(File, request.name)

    if existing_file is not None:
        return AdminCreateFileResponse(already_exists=True, success=True)

    # Check the file instance exists.
    full_path = Path(request.path)

    if not full_path.exists():
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason=f"File {full_path} does not exist.",
            suggested_remedy="Create the file first, or make sure that you are using a local store.",
        )

    # Create the file and instance.
    new_file = File.new_file(
        filename=request.name,
        size=request.size,
        checksum=request.checksum,
        uploader=request.uploader,
        source=request.source,
    )

    new_instance = Instance.new_instance(
        path=request.path,
        file=new_file,
        deletion_policy=DeletionPolicy.DISALLOWED,
        store=store,
    )

    session.add_all([new_file, new_instance])

    session.commit()

    return AdminCreateFileResponse(success=True, file_exists=True)


@router.post(
    "/verify_file",
    response_model=Union[FileVerificationResponse, AdminRequestFailedResponse],
)
def verify_file(
    request: AdminVerifyFileRequest,
    response: Response,
    session: Session = Depends(yield_session),
) -> Union[FileVerificationResponse, AdminRequestFailedResponse]:
    """
    Verifies the properties of an existing file in the database and returns newly computed checksums and sizes for all of the instances.
    If the requested store or file does not exist, or if no instances are found, it returns a 400 Bad Request response.
    """

    file = session.get(File, request.name)
    if file is None:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason="File not found.",
            suggested_remedy="Ensure the file exists in the database.",
        )

    instances = file.instances
    if not instances:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return AdminRequestFailedResponse(
            reason="File instances not found.",
            suggested_remedy="Ensure the file has instances in the database.",
        )

    checksums_and_sizes = []
    for instance in instances:
        path = instance.path
        checksum = get_md5_from_path(path)
        size = get_size_from_path(path)
        checksums_and_sizes.append(
            {
                "store_id": str(instance.store_id),
                "checksum": checksum,
                "size": str(size),
            }
        )
    return FileVerificationResponse(
        verified=True, checksums_and_sizes=checksums_and_sizes
    )
