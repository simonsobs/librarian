"""
Repairs a downstream librarian database by using remote instance and
outgoing transfer data from an upstream librarian.

This script is best used when your downstream database is missing
information. You should recover it from a backup, and then use this
to 'migrate' any missing items.

Other steps to take:

1. Stop the upstream librarian from generating new outgoing transfers
   but allow it to complete those in-flight. Do not delete hanging
   staging directories until the transfers have all 'gone through'.
2. Review your backup schedule.
"""

# This script recreates three major sets of rows:
# 1. The file rows
# 2. The instance rows
# 3. The IncomingTransfer rows
#
# The first two are generated from RemoteInstances on the source
# and the latter is re-created from the OutgoingTransfer rows on
# the source.


import argparse as ap
import datetime
from pathlib import Path

from pydantic import BaseModel

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.transfer import TransferStatus
from librarian_server import database, orm

parser = ap.ArgumentParser(
    description=(
        "Repair the librarian database. This script runs in two modes: source, "
        "and destination. At the source, you will produce a file that must be "
        "out-of-band transferred to the destination (or you can use unix pipes...) "
        "and can be ingested using 'destination'"
    )
)

# SOURCE Arguments
parser.add_argument(
    "--source",
    help="Run the script in source mode. Produces a dump of databases",
    action="store_true",
)

parser.add_argument(
    "--librarian-name",
    help="The librarian name that you would like to extract data for",
    type=str,
)

parser.add_argument(
    "--age",
    help="The age (in HOURS) to go back in time and select remote instances "
    "and outbound transfers for.",
    type=float,
)

# DESTINATION Arguments
parser.add_argument(
    "--destination",
    help="Name of the store to re-build.",
    action="store_true",
)


class FileInfo(BaseModel):
    name: str
    store_id: int
    copy_time: datetime
    size: int
    checksum: str
    uploader: str
    # Note source should be set to the librarian that generates this info
    source: str

    @classmethod
    def from_file(
        cls, file: orm.File, remote_instance: orm.RemoteInstance, source: str
    ) -> "FileInfo":
        return FileInfo(
            name=file.name,
            store_id=remote_instance.store_id,
            copy_time=remote_instance.copy_time,
            size=file.size,
            checksum=file.checksum,
            uploader=file.uploader,
            source=source,
        )

    def to_file(self, store: orm.StoreMetadata) -> tuple[orm.File, orm.Instance]:
        instance = orm.Instance(
            path=str(store.store_manager.resolve_path_store(Path(self.name))),
            file_name=self.name,
            store=store,
            deletion_policy=DeletionPolicy.DISALLOWED,
            created_time=self.copy_time,
            available=True,
        )

        file = orm.File(
            name=self.name,
            create_time=self.create_time,
            size=self.size,
            checksum=self.checksum,
            uploader=self.uploader,
            source=self.source,
            instances=[instance],
        )

        return file, instance


class TransferInfo(BaseModel):
    source_id: int
    destination_id: int
    status: TransferStatus
    transfer_size: int
    transfer_checksum: int
    transfer_manager_name: str
    start_time: datetime
    file_name: str
    source: str
    uploader: str
    dest_path: str

    @classmethod
    def from_transfer(
        cls, file: orm.File, outgoing_transfer: orm.OutgoingTransfer, source: str
    ) -> "TransferInfo":
        return TransferInfo(
            source_id=outgoing_transfer.id,
            destination_id=outgoing_transfer.remote_transfer_id,
            status=outgoing_transfer.status,
            transfer_size=outgoing_transfer.transfer_size,
            transfer_checksum=outgoing_transfer.transfer_checksum,
            transfer_manager_name=outgoing_transfer.transfer_manager_name,
            start_time=outgoing_transfer.start_time,
            file_name=outgoing_transfer.file_name,
            source=source,
            uploader=file.uploader,
            dest_path=outgoing_transfer.dest_path,
        )

    def to_transfer(self, store: orm.StoreMetadata) -> orm.IncomingTransfer:
        return orm.IncomingTransfer(
            id=self.destination_id,
            status=self.status,
            uploader=self.uploader,
            upload_name=self.file_name,
            source=self.source,
            transfer_size=self.transfer_size,
            transfer_checksum=self.transfer_checksum,
            store=store,
            transfer_manager_name=self.transfer_manager_name,
            start_time=self.start_time,
            # TODO: Check if this is just the UUID or not? We can get that with relative paths.
            staging_path=self.dest_path,
            store_path=store.store_manager.resolve_path_store(Path(self.file_name)),
            source_transfer_id=self.source_id,
        )


def main():
    args = parser.parse_args()

    if args.source and args.destination:
        raise ValueError(
            "Can not have both source and destination mode activated at the same time"
        )

    if (not args.source) and (not args.destination):
        raise ValueError("Please select one, destination or source.")

    if args.source:
        core_source(
            librarian_name=args.librarian_name,
            age=args.age,
        )


def core_source(librarian_name: str, age: float):
    """
    Generates (and prints) a JSON representaion remote instances and
    outgoing transfers for ingest on destination side.
    """
    return


if __name__ == "__main__":
    main()
