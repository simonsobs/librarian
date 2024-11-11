"""
The twin of send_clone.py, this file contains the code for recieving a clone
from a remote librarian. We loop through the incoming transfers and check
to see if they have completed.
"""

import datetime
import logging
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from sqlalchemy.orm import Session

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.exceptions import LibrarianHTTPError
from hera_librarian.models.clone import CloneCompleteRequest, CloneCompleteResponse
from librarian_server.database import get_session
from librarian_server.logger import ErrorCategory, ErrorSeverity, log_to_database
from librarian_server.orm import (
    File,
    IncomingTransfer,
    Instance,
    Librarian,
    StoreMetadata,
    TransferStatus,
)

from .task import Task

logger = logging.getLogger("schedule")


class RecieveClone(Task):
    """
    Recieves incoming files from other librarians.
    """

    deletion_policy: DeletionPolicy = DeletionPolicy.DISALLOWED
    "The deletion policy for ingested instances."
    files_per_run: int = 1024
    "The number of files to process per run."

    def on_call(self):  # pragma: no cover
        with get_session() as session:
            return self.core(session=session)

    def core(self, session: Session):
        """
        Checks for incoming transfers and processes them.
        """

        core_begin = datetime.datetime.now(datetime.timezone.utc)

        # Find incoming transfers that are STAGED
        ongoing_transfers: list[IncomingTransfer] = (
            session.query(IncomingTransfer)
            .filter_by(status=TransferStatus.STAGED)
            .all()
        )

        all_transfers_succeeded = True

        if len(ongoing_transfers) == 0:
            logger.info("No ongoing transfers to process.")

        transfers_processed = 0

        for transfer in ongoing_transfers:
            if (
                (
                    datetime.datetime.now(datetime.timezone.utc) - core_begin
                    > self.soft_timeout
                )
                if self.soft_timeout
                else False
            ):
                logger.info(
                    "RecieveClone task has gone over time. Will reschedule for later."
                )
                break

            if transfers_processed >= self.files_per_run:
                logger.info(
                    f"Processed {transfers_processed} transfers, which is the maximum for this run."
                )
                break

            # Check if the transfer has completed
            store: StoreMetadata = transfer.store

            if store is None:
                log_to_database(
                    severity=ErrorSeverity.CRITICAL,
                    category=ErrorCategory.PROGRAMMING,
                    message=(
                        f"Transfer {transfer.id} has no store associated with it. "
                        "Skipping for now, but this should never happen."
                    ),
                    session=session,
                )

                all_transfers_succeeded = False

                continue

            try:
                store.ingest_staged_file(
                    transfer=transfer,
                    session=session,
                    deletion_policy=self.deletion_policy,
                )
            except (FileNotFoundError, FileExistsError, ValueError) as e:
                log_to_database(
                    severity=ErrorSeverity.ERROR,
                    category=ErrorCategory.PROGRAMMING,
                    message=traceback.format_exc(),
                    session=session,
                )

                all_transfers_succeeded = False

                continue

            # Mark the transfer as completed.
            transfer.status = TransferStatus.COMPLETED
            transfer.end_time = datetime.datetime.now(datetime.timezone.utc)

            # Commit the changes.
            session.commit()

            # Callback to the source librarian.
            librarian: Optional[Librarian] = (
                session.query(Librarian).filter_by(name=transfer.source).first()
            )

            if librarian:
                # Need to call back
                logger.info(
                    f"Transfer {transfer.id} has completed. Calling back to librarian {librarian.name}."
                )

                request = CloneCompleteRequest(
                    source_transfer_id=transfer.source_transfer_id,
                    destination_transfer_id=transfer.id,
                    store_id=store.id,
                )

                logger.debug(f"Request to send: {request}")

                downstream_client = librarian.client()

                try:
                    logger.info("Sending clone complete request.")
                    response: CloneCompleteResponse = downstream_client.post(
                        endpoint="clone/complete",
                        request=request,
                        response=CloneCompleteResponse,
                    )
                except LibrarianHTTPError as e:
                    log_to_database(
                        severity=ErrorSeverity.ERROR,
                        category=ErrorCategory.LIBRARIAN_NETWORK_AVAILABILITY,
                        message=(
                            f"Failed to call back to librarian {librarian.name} "
                            f"with exception {e}."
                        ),
                        session=session,
                    )
            else:
                logger.error(
                    f"Transfer {transfer.id} has no source librarian "
                    f"(source is {transfer.source}) - cannot callback."
                )

            transfers_processed += 1

        return all_transfers_succeeded
