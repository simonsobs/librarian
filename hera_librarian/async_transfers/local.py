"""
The local async transfer manager.
"""

import copy
import os
import shutil
from pathlib import Path
from socket import gethostname

from hera_librarian.transfer import TransferStatus

from .core import CoreAsyncTransferManager

# importing other libraries
import time
from datetime import datetime
from typing import Optional


class LocalAsyncTransferManager(CoreAsyncTransferManager):
    hostnames: list[str]

    transfer_attempted: bool = False
    transfer_complete: bool = False

    # Adding the transfer attributes
    start_time_transfer: Optional[float] = None
    bytes_transfer: Optional[int] = None

    def batch_transfer(self, paths: list[tuple[Path]], settings: "ServerSettings"):
        # Begin initialization of time and total bytes
        self.start_time_transfer = time.time()
        total_bytes = 0

        for local_path, _ in paths:
            if local_path.is_dir():
                total_bytes += sum(
                    f.stat().st_size for f in local_path.glob("**/*") if f.is_file()
                )
            else:
                total_bytes += local_path.stat().st_size

        self.bytes_transfer = total_bytes
        # End of initializations

        copy_success = True

        for local_path, remote_path in paths:
            copy_success = copy_success and self.transfer(
                local_path=local_path, remote_path=remote_path, settings=settings
            )

        # Set local
        self.transfer_attempted = True
        self.transfer_complete = copy_success

        return copy_success

    def transfer(self, local_path: Path, remote_path: Path, settings: "ServerSettings"):
        """
        Transfer a file from the local filesystem to the remote filesystem.

        Parameters
        ----------
        local_path : Path
            The path to the local file.
        remote_path : Path
            The path to the file on the remote machine.

        Raises
        ------

        ValueError
            If the transfer fails.
        PermissionError
            If the permissions cannot be set.
        """

        # Need to make sure that the the permissions are correctly
        # set on all files and directories that we copy over.
        # They should have rw-rw-r-- and rwxrwxr-x permissions.

        # Note that async transfers for some reason copy the whole file path.
        # They don't just use the base-level, so we need to create the directory
        # structure from bottom-up.
        dest_path = copy.copy(remote_path).parent
        paths_to_modify = []

        while not dest_path.exists():
            dest_path.mkdir(mode=0o775)
            paths_to_modify.append(copy.copy(dest_path))
            dest_path = dest_path.parent

        # Get the group of the parent.
        parent_group = dest_path.stat().st_gid
        # Get this user's uid.
        uid = os.getuid()

        def set_for_file(file: Path):
            if file.is_dir():
                os.chmod(
                    file,
                    0o775,
                )
            else:
                os.chmod(file, 0o664)

            os.chown(file, uid=uid, gid=parent_group)

            return

        for path in paths_to_modify:
            set_for_file(path)

        copy_success = False

        if local_path.is_dir():
            # Note that dirs_exist_ok is not acceptable here for the
            # case where there is a folder that is being used as the File.
            copy_success = shutil.copytree(local_path, remote_path)
        else:
            # Copy2 copies more metadata.
            copy_success = shutil.copy2(local_path, remote_path)

        if not copy_success:
            raise ValueError(f"Could not copy {local_path} to {remote_path}")

        # Set base permission
        set_for_file(remote_path)

        if remote_path.is_dir():
            for root, dirs, files in os.walk(remote_path):
                for x in dirs + files:
                    set_for_file(Path(root) / x)

        return True

    def valid(self, settings: "ServerSettings") -> bool:
        return gethostname() in self.hostnames

    def transfer_status(self, settings: "ServerSettings") -> TransferStatus:
        if self.transfer_complete:
            return TransferStatus.COMPLETED
        else:
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED

    def fail_transfer(self, settings: "ServerSettings") -> bool:
        return True

    def complete_transfer(self) -> dict | None:
        """ """
        # 1. Check if the transfer metrics were recorded
        if self.start_time_transfer is None or self.bytes_transfer is None:
            print("Error: Transfer metrics were not recorded.")
            return None

        # 2. Calculate performance metrics from the recorded data
        end_time = time.time()
        duration_seconds = end_time - self.start_time_transfer
        total_bytes = self.bytes_transfer

        if duration_seconds > 0:
            bandwidth_mbps = (total_bytes * 8) / (duration_seconds * 1_000_000)
        else:
            bandwidth_mbps = -1

        # 3. Create and return the report dictionary
        transfer_report = {
            "task_id": f"local_{int(end_time)}",
            "source_endpoint_id": gethostname(),
            "destination_endpoint_id": gethostname(),
            "start_time": datetime.fromtimestamp(
                self.start_time_transfer, tz=timezone.utc
            ),
            "end_time": datetime.fromtimestamp(end_time, tz=timezone.utc),
            "duration_seconds": duration_seconds,
            "bytes_transferred": total_bytes,
            "effective_bandwidth_mbps": round(bandwidth_mbps, 2),
        }

        return transfer_report
