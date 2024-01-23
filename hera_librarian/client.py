"""
The public-facing LibrarianClient object.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Optional

import requests
from pydantic import BaseModel

from .deletion import DeletionPolicy
from .exceptions import LibrarianError, LibrarianHTTPError
from .models.ping import PingRequest, PingResponse
from .models.uploads import (UploadCompletionRequest, UploadInitiationRequest,
                             UploadInitiationResponse)
from .utils import get_md5_from_path, get_size_from_path

if TYPE_CHECKING:
    from .transfers import CoreTransferManager


class LibrarianClient:
    """
    A client for the Librarian API.
    """

    host: str
    port: int
    user: str

    def __init__(self, host: str, port: int, user: str):
        """
        Create a new LibrarianClient.

        Parameters
        ----------
        host : str
            The hostname of the Librarian server.
        port : int
            The port of the Librarian server.
        user : str
            The name of the user.
        """

        if host[-1] == "/":
            self.host = host[:-1]
        else:
            self.host = host

        self.port = port
        self.user = user

    def __repr__(self):
        return f"Librarian Client ({self.user}) for {self.host}:{self.port}"

    @property
    def hostname(self):
        return f"{self.host}:{self.port}/api/v2"

    def resolve(self, path: str):
        """
        Resolve a path to a URL.

        Parameters
        ----------
        path : str
            The path to resolve.

        Returns
        -------
        str
            The resolved URL.
        """

        if path[0] == "/":
            return f"{self.hostname}{path}"
        else:
            return f"{self.hostname}/{path}"

    def post(
        self,
        endpoint: str,
        request: Optional[BaseModel] = None,
        response: Optional[BaseModel] = None,
    ) -> Optional[BaseModel]:
        """
        Do a POST operation, passing a JSON version of the request and expecting a
        JSON reply; return the decoded version of the latter.

        Parameters
        ----------
        endpoint : str
            The endpoint to post to.
        request : pydantic.BaseModel, optional
            The request model to send. If None, we don't ask for anything.
        response : pydantic.BaseModel, optional
            The response model to expect. If None, we don't return anything.

        Returns
        -------
        response, optional
            The decoded response model, or None.

        Raises
        ------

        LibrarianHTTPError
            If the HTTP request fails.

        pydantic.ValidationError
            If the remote librarian returns an invalid response.
        """

        data = None if request is None else request.model_dump_json()

        r = requests.post(
            self.resolve(endpoint),
            data=data,
            headers={"Content-Type": "application/json"},
        )

        if str(r.status_code)[0] != "2":
            try:
                json = r.json()
            except requests.exceptions.JSONDecodeError:
                json = {}

            raise LibrarianHTTPError(
                url=endpoint,
                status_code=r.status_code,
                reason=json.get("reason", "<no reason provided>"),
                suggested_remedy=json.get(
                    "suggested_remedy", "<no suggested remedy provided>"
                ),
            )

        if response is None:
            return None
        else:
            # Note that the pydantic model wants the full bytes content
            # not the deserialized r.json()
            return response.model_validate_json(r.content)

    def ping(self) -> PingResponse:
        """
        Ping the remote librarian to see if it exists.

        Returns
        -------

        PingResponse
            The response from the remote librarian.

        Raises
        ------

        LibrarianHTTPError
            If the remote librarian is unreachable.

        pydantic.ValidationError
            If the remote librarian returns an invalid response.
        """

        response: PingResponse = self.post(
            endpoint="ping",
            request=PingRequest(),
            response=PingResponse,
        )

        return response

    def upload(
        self,
        local_path: Path,
        dest_path: Path,
        deletion_policy: DeletionPolicy | str = DeletionPolicy.DISALLOWED,
    ):
        """
        Upload a file or directory to the librarian.

        Parameters
        ----------
        local_path : Path
            Path of the file or directory to upload.
        dest_path : Path
            The destination 'path' on the librarian store (often the same as your filename, but may be under some root directory).
        deletion_policy : DeletionPolicy | str, optional
            Whether or not this file may be deleted, by default DeletionPolicy.DISALLOWED

        Returns
        -------
        dict
            _description_

        Raises
        ------
        ValueError
            If the provided path is incorrect.
        LibrarianError:
            If the remote librarian cannot be transferred to.
        """

        if isinstance(deletion_policy, str):
            deletion_policy = DeletionPolicy.from_str(deletion_policy)

        if dest_path.is_absolute():
            raise ValueError(f"Destination path may not be absolute; got {dest_path}")

        # Ask the librarian for a staging directory, and a list of transfer managers
        # to try.

        response: UploadInitiationResponse = self.post(
            endpoint="upload/stage",
            request=UploadInitiationRequest(
                upload_size=get_size_from_path(local_path),
                upload_checksum=get_md5_from_path(local_path),
                upload_name=dest_path.name,
                destination_location=dest_path,
                uploader=self.user,
            ),
            response=UploadInitiationResponse,
        )

        transfer_managers = response.transfer_providers

        # Now try all the transfer managers. If they're valid, we try to use them.
        # If they fail, we should probably catch the exception.
        # TODO: Catch the exception on failure.
        used_transfer_manager: Optional["CoreTransferManager"] = None
        used_transfer_manager_name: Optional[str] = None

        # TODO: Should probably have some manual ordering here.
        for name, transfer_manager in transfer_managers.items():
            if transfer_manager.valid:
                transfer_manager.transfer(
                    local_path=local_path, remote_path=response.staging_location
                )

                # We used this.
                used_transfer_manager = transfer_manager
                used_transfer_manager_name = name

                break
            else:
                print(f"Warning: transfer manager {name} is not valid.")

        if used_transfer_manager is None:
            raise LibrarianError("No valid transfer managers found.")

        # If we made it here, the file is successfully on the store!
        request = UploadCompletionRequest(
            store_name=response.store_name,
            staging_name=response.staging_name,
            staging_location=response.staging_location,
            upload_name=response.upload_name,
            destination_location=dest_path,
            transfer_provider_name=used_transfer_manager_name,
            transfer_provider=used_transfer_manager,
            # Note: meta_mode is used in current status
            meta_mode="infer",
            deletion_policy=deletion_policy,
            source_name=self.user,
            # Note: we ALWAYS use null_obsid
            null_obsid=True,
            uploader=self.user,
            transfer_id=response.transfer_id,
        )

        self.post(
            endpoint="upload/commit",
            request=request,
        )

        return
