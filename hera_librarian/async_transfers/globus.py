"""
A transfer manager for Globus transfers.
"""

import os
from datetime import datetime
from pathlib import Path

import globus_sdk
from loguru import logger

from hera_librarian.models.transfer import CompletedTransferCore
from hera_librarian.transfer import TransferStatus
from hera_librarian.utils import GLOBUS_ERROR_EVENTS

from .core import CoreAsyncTransferManager


class GlobusAsyncTransferManager(CoreAsyncTransferManager):
    """
    A transfer manager that uses Globus. This requires the
    local endpoint, the destiation endpoint, and the secret
    for authentication.
    """

    destination_endpoint: str
    # The Globus endpoint UUID for the destination, entered in the configuration.

    native_app: bool = False
    # Whether to use a Native App (true) or a Confidential App (false, default)
    # for authorizing the client.

    transfer_attempted: bool = False
    transfer_complete: bool = False
    task_id: str = ""

    @staticmethod
    def _subtract_local_root(path: Path, settings: "ServerSettings") -> Path:
        """
        This is a helper function to finalize the remote path for a transfer.
        """
        if settings.globus_local_root is None:
            return path
        else:
            return path.relative_to(settings.globus_local_root)

    def authorize(self, settings: "ServerSettings"):
        """
        Attempt to authorize using the Globus service.

        This method will attempt to authenticate with Globus. There are two
        primary objects that can be used for this: the NativeAppAuthClient, and
        the ConfidentialAppAuthClient. The Native App is used for having a user
        authenticate as "themselves", and is tied to a "thick client". The
        Confidential App is used for having the client authenticate as "itself",
        and is a "service account" not explicitly tied to a specific Globus user
        account.

        Note that the "secret" used is different in the two cases: for the
        Native App, the secret is assumed to be a Refresh Token, which is a
        long-lived token that allows the user to authenticate and initiate
        transfers. For the Confidential App, the secret is the Client Secret
        generated when the app was created.

        Once the authenticator is created, the way they work downstream is
        effectively interchangeable. Note that Globus as a service will perform
        further checking to see if the user/app has permission to read and write
        to specific endpoints. We will do our best to handle this as it comes up
        to provide the user with nicer error messages, though we may not have
        caught all possible failure modes.

        Parameters
        ----------
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        Globus authorizer or None
            The object returned will be an instance of
            globus_sdk.RefreshTokenAuthorizer (if using the Native App),
            globus_sdk.AccessTokenAuthorizer (if using the Confidential App),
            or None (if we could not successfully authenticate).
        """
        if settings.globus_enable is False:
            return None

        if settings.globus_client_native_app:
            try:
                client = globus_sdk.NativeAppAuthClient(
                    client_id=settings.globus_client_id
                )
                authorizer = globus_sdk.RefreshTokenAuthorizer(
                    refresh_token=settings.globus_client_secret, auth_client=client
                )
            except globus_sdk.AuthAPIError:
                return None
        else:
            try:
                client = globus_sdk.ConfidentialAppAuthClient(
                    client_id=settings.globus_client_id,
                    client_secret=settings.globus_client_secret,
                )
                tokens = client.oauth2_client_credentials_tokens()
                transfer_tokens_info = tokens.by_resource_server[
                    "transfer.api.globus.org"
                ]
                transfer_token = transfer_tokens_info["access_token"]
                authorizer = globus_sdk.AccessTokenAuthorizer(
                    access_token=transfer_token
                )
            except globus_sdk.AuthAPIError:
                return None

        return authorizer

    def valid(self, settings: "ServerSettings") -> bool:
        """
        Test whether it's valid to use Globus or not.

        Technically this only checks that we can authenticate with Globus and
        does not verify that we can copy files between specific endpoints.
        However, this is an important starting point and can fail for reasons of
        network connectivity, Globus as a service being down, etc.

        Parameters
        ----------
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        bool
            Whether we can authenticate with Globus (True) or not (False).
        """
        authorizer = self.authorize(settings=settings)
        return authorizer is not None

    def _get_transfer_data(self, label: str, settings: "ServerSettings"):
        """
        This is a helper function to create a TransferData object, which is
        needed both for single-book transfers and batch transfers.
        """
        # create a TransferData object that contains options for the transfer
        transfer_data = globus_sdk.TransferData(
            source_endpoint=settings.globus_local_endpoint_id,
            destination_endpoint=self.destination_endpoint,
            label=label,
            sync_level="exists",
            verify_checksum=True,  # We do this ourselves, but globus will auto-retry if it detects failed files
            preserve_timestamp=True,
            notify_on_succeeded=False,
            skip_source_errors=False,
            fail_on_quota_errors=True,
            encrypt_data=settings.globus_encrypt_transfers,
        )

        return transfer_data

    def transfer(
        self,
        local_path: Path,
        remote_path: Path,
        settings: "ServerSettings",
    ) -> bool:
        """
        Attempt to transfer a book using Globus.

        This method will attempt to create a Globus transfer. If successful, we
        will have set the task ID of the transfer on the object, which can be
        used to query Globus as to its status. If unsuccessful, we will have
        gotten nothing but sadness.

        Parameters
        ----------
        local_path : Path
            The local path for the transfer relative to the root Globus
            directory, which is generally not the same as /.
        remote_path : Path
            The remote path for the transfer relative to the root Globus
            directory, which is generally not the same as /.
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        bool
            Whether we could successfully initiate a transfer (True) or not (False).
        """
        self.transfer_attempted = True

        # start by authorizing
        authorizer = self.authorize(settings=settings)
        if authorizer is None:
            return False

        # create a label from the name of the book
        label = os.path.basename(local_path)

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

        # get a TransferData object
        transfer_data = self._get_transfer_data(label=label, settings=settings)

        # We need to figure out if the local path is actually a directory or a
        # flat file, which annoyingly requires different handling as part of the
        # Globus transfer.
        relative_local_path = self._subtract_local_root(local_path, settings)
        transfer_data.add_item(
            str(relative_local_path), str(remote_path), recursive=local_path.is_dir()
        )

        # try to submit the task
        try:
            task_doc = transfer_client.submit_transfer(transfer_data)
        except globus_sdk.TransferAPIError:
            return False

        self.task_id = task_doc["task_id"]

        return True

    def batch_transfer(
        self,
        paths: list[tuple[Path]],
        settings: "ServerSettings",
    ) -> bool:
        """
        Attempt to transfer a series of books using Globus.

        This method will attempt to create a Globus transfer. If successful, we
        will have set the task ID of the transfer on the object, which can be
        used to query Globus as to its status. If unsuccessful, we will have
        gotten nothing but sadness.

        Parameters
        ----------
        paths : list of tuples of Paths
            A series of length-2 tuples containing pairs of local and remote
            Paths to include as part of the transfer.
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        bool
            Whether we could successfully initiate a transfer (True) or not
            (False).

        """
        self.transfer_attempted = True

        # We have to do a lot of the same legwork as above for a single
        # transfer, with the biggest change being that we can add multiple items
        # to a single TransferData object. This is effectively how we "batch"
        # books using Globus.

        # start by authorizing
        authorizer = self.authorize(settings=settings)
        if authorizer is None:
            return False

        # make a label from the first book
        label = "batch with " + os.path.basename(paths[0][0])

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

        # get a TransferData object
        transfer_data = self._get_transfer_data(label=label, settings=settings)

        # add each of our books to our task
        for local_path, remote_path in paths:
            # We need to figure out if the local path is actually a directory or a
            # flat file, which annoyingly requires different handling as part of the
            # Globus transfer.
            relative_local_path = self._subtract_local_root(local_path, settings)
            transfer_data.add_item(
                str(relative_local_path),
                str(remote_path),
                recursive=local_path.is_dir(),
            )

        # submit the transfer
        try:
            task_doc = transfer_client.submit_transfer(transfer_data)
        except globus_sdk.TransferAPIError:
            return False

        self.task_id = task_doc["task_id"]
        return True

    def transfer_status(self, settings: "ServerSettings") -> TransferStatus:
        """
        Query Globus to see if our transfer has finished yet.

        Parameters
        ----------
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        TransferStatus
            The status of the relevant transfer. Should be one of: INITIATED (if
            the transfer has not yet been started, or is in-flight), SUCCEEDED
            (if the transfer was successful), or FAILED (if the transfer was
            unsuccessful, we could not contact Globus, or if the transfer was
            attempted but could not be completed).
        """
        authorizer = self.authorize(settings=settings)
        if authorizer is None:
            # We *should* be able to just assume that we have already
            # authenticated and should be able to query the status of our
            # transfer. However, if for whatever reason we're not able to talk
            # to Globus (network issues, Globus outage, etc.), we won't be able
            # to find out our transfer's status -- let's bail and assume we
            # failed
            return TransferStatus.FAILED

        if self.task_id == "":
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED
        else:
            # start talking to Globus
            transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
            task_doc = transfer_client.get_task(self.task_id)

            if task_doc["status"] == "SUCCEEDED":
                return TransferStatus.COMPLETED
            elif task_doc["status"] == "FAILED":
                # Log the error, Globus transfers should not fail.
                logger.error(
                    "Task {task_id} failed ({code}: {reason}), {nice_status}",
                    task_id=task_doc["task_id"],
                    code=task_doc.get("fatal_error", {}).get("code", None),
                    reason=task_doc.get("fatal_error", {}).get("description", None),
                    nice_status=task_doc.get("nice_status", None),
                )
                return TransferStatus.FAILED
            # When there are errors, better fail the task and try again. There is
            # a different check for faults to make the state transition as clear as
            # possible.
            elif task_doc["faults"] > 0:
                task_event_list = transfer_client.task_event_list(self.task_id)
                for event in task_event_list:
                    if event["code"] in GLOBUS_ERROR_EVENTS and event["is_error"]:
                        # Log the error, Globus transfers should not fail.
                        logger.error(
                            "Task {task_id} failed ({code}), {nice_status}",
                            task_id=task_doc["task_id"],
                            code=event.get["code"],
                            nice_status=task_doc.get("nice_status", None),
                        )
                        return TransferStatus.FAILED
                return TransferStatus.FAILED
            else:  # "status" == "ACTIVE"
                return TransferStatus.INITIATED

    def fail_transfer(self, settings: "ServerSettings") -> bool:
        """
        A GLobus task needs to be canceled because it has errors.

        Parameters
        ----------
        settings : ServerSettings object
            The settings for the Librarian server. These settings should include
            the Globus login information.

        Returns
        -------
        bool
            Whether we could successfully cancelled a transfer (True) or not
            (False).

        """
        authorizer = self.authorize(settings=settings)
        if authorizer is None:
            # We *should* be able to just assume that we have already
            # authenticated and should be able to query the status of our
            # transfer. However, if for whatever reason we're not able to talk
            # to Globus (network issues, Globus outage, etc.), we won't be able
            # to find out our transfer's status -- let's bail and assume we
            # failed
            return False

        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

        try:
            _ = transfer_client.cancel_task(self.task_id)
        except globus_sdk.TransferAPIError:
            return False
        return True

    def gather_transfer_details(
        self, settings: "ServerSettings"
    ) -> CompletedTransferCore | None:
        """
        Gathers details about a completed transfer from Globus and
        returns them in a Pydantic object.
        """

        authorizer = self.authorize(settings=settings)
        if authorizer is None:
            logger.debug("Authorizer not provided, attempting internal authorization")

        if not authorizer:
            logger.error("Authorization failed")
            return None

        logger.debug("Authorization successful")

        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)

        try:
            logger.debug(f"Fetching task details for ID: {self.task_id}")
            task_doc = transfer_client.get_task(self.task_id)
            logger.debug(f"Task data fetched. Status is: {task_doc['status']}")
        except globus_sdk.TransferAPIError as e:
            logger.error(f"Globus API Error when fetching task: {e}")
            return None

        if task_doc["status"] != "SUCCEEDED":
            logger.warning("Task status is not SUCCEEDED.")
            return None

        start_time = datetime.fromisoformat(task_doc["request_time"])
        end_time = datetime.fromisoformat(task_doc["completion_time"])
        bytes_transferred = task_doc["bytes_transferred"]
        bandwidth_bps = task_doc["effective_bytes_per_second"]

        duration = end_time - start_time
        duration_seconds = duration.total_seconds()

        try:
            transfer_record = CompletedTransferCore(
                task_id=task_doc["task_id"],
                source_endpoint_id=task_doc["source_endpoint_id"],
                destination_endpoint_id=task_doc["destination_endpoint_id"],
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                bytes_transferred=bytes_transferred,
                effective_bandwidth_bps=bandwidth_bps,
            )
            return transfer_record
        except (KeyError, ValueError) as e:
            logger.error(
                f"Missing key or malformed value: {e} related to task {self.task_id}"
            )
