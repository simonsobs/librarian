"""
The simplest possible store is a local store, which just executes
all commands on the machine local to the librarian server.
"""

import os
import shutil
import stat
import uuid
from pathlib import Path

from loguru import logger

from hera_librarian.transfers.core import CoreTransferManager
from hera_librarian.utils import (
    compare_checksums,
    get_checksum_from_path,
    get_size_from_path,
    get_type_from_path,
)

from .core import CoreStore
from .pathinfo import PathInfo


class LocalStore(CoreStore):
    staging_path: Path
    store_path: Path

    report_full_fraction: float = 1.0
    "The fraction of the store that must be full before we report it as full. 1.0 means 100% full. Typical to set 0.9 or 0.95."

    group_write_after_stage: bool = False
    "If true, the user running the server will chmod the stage directories to 775 after creating."
    own_after_commit: bool = False
    "If true, the user running the server will chown the files after committing."
    readonly_after_commit: bool = False
    "If true, the user running the server will chmod the files to 444 and folders to 555 after commit."

    max_copy_retries: int = 3

    @property
    def available(self) -> bool:
        try:
            if not self.staging_path.exists():
                return False  # We don't have a staging area.

            if not self.store_path.exists():
                return False
        except (OSError, FileNotFoundError):
            return False

        return True

    @property
    def free_space(self) -> int:
        if not self.available:
            return -1

        store = shutil.disk_usage(self.store_path)
        staging = shutil.disk_usage(self.staging_path)

        reserved_fraction = 1.0 - self.report_full_fraction

        store_free = store.free - (store.total * reserved_fraction)
        staging_free = staging.free - (staging.total * reserved_fraction)

        return int(max(min(store_free, staging_free), 0))

    def _resolved_path_staging(self, path: Path) -> Path:
        if not path.is_absolute():
            complete_path = (self.staging_path / path).resolve()
        else:
            complete_path = path.resolve()

        # Check if the file is validly in our staging area. Someone
        # could pass us ../../../../../../etc/passwd or something.

        if not (self.staging_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside staging area.")

        return complete_path

    def _resolved_path_store(self, path: Path) -> Path:
        if not path.is_absolute():
            complete_path = (self.store_path / path).resolve()
        else:
            complete_path = path.resolve()

        if not (self.store_path.resolve() in complete_path.parents):
            raise ValueError(f"Provided path {path} resolves outside store area.")

        return complete_path

    def resolve_path_store(self, path: Path | str) -> Path:
        return self._resolved_path_store(Path(path))

    def resolve_path_staging(self, path: Path | str) -> Path:
        return self._resolved_path_staging(Path(path))

    def stage(self, file_size: int, file_name: Path) -> tuple[Path]:
        if file_size > self.free_space:
            logger.error(
                "Not enough free space on store. Requested: {}, Available: {}",
                file_size,
                self.free_space,
            )
            raise ValueError("Not enough free space on store")

        # TODO: Do we want to actually keep track of files we have staged?
        #       Also maybe we want to check if the staging area is clear at startup?

        stage_path = Path(f"{uuid.uuid4()}")

        # Create the empty directory.
        resolved_path = self._resolved_path_staging(stage_path)

        if self.group_write_after_stage:
            resolved_path.mkdir(mode=0o775)
            os.chmod(resolved_path, 0o775)
        else:
            resolved_path.mkdir()

        return stage_path, resolved_path / file_name

    def unstage(self, path: Path):
        complete_path = self._resolved_path_staging(path)

        if os.path.exists(complete_path):
            try:
                os.rmdir(complete_path)
            except NotADirectoryError:
                # It's not a directory. Delete it.
                os.remove(complete_path)
            except OSError:
                # Directory is not empty. Delete it and all its contents.
                logger.info(
                    f"Directory {complete_path} is not empty. Deleting all contents"
                )
                shutil.rmtree(complete_path)

        # Check if the parent is still in the staging area. We don't want
        # to leave random dregs around!

        if os.path.exists(complete_path.parent):
            try:
                resolved_path = self._resolved_path_staging(complete_path.parent)

                if any(resolved_path.iterdir()):
                    raise ValueError("Parent directory is not empty.")

                resolved_path.rmdir()
            except ValueError:
                # The parent is not in the staging area. We can't delete it.
                pass

        return

    def remove_readonly_store(self, func, path, _):
        """
        Clear the readonly bit and reattempt the removal, see
        https://docs.python.org/3/library/shutil.html#rmtree-example.

        Makes the file writeable by all, and also checks the parent
        (if it is in the store area) and ensures it has the correct
        +x directory bit set.
        """
        os.chmod(
            path,
            stat.S_IREAD
            | stat.S_IWRITE
            | stat.S_IRGRP
            | stat.S_IWGRP
            | stat.S_IROTH
            | stat.S_IWOTH,
        )

        parent = os.path.dirname(path)
        parent_stat = os.stat(parent)
        parent_imode = stat.S_IMODE(parent_stat.st_mode)
        set_permissions = False

        try:
            self.resolve_path_store(Path(parent))
        except ValueError:
            # Parent is _not_ in store.
            func(path)
            return

        # Check if +x bit is already set:
        if not (parent_imode & stat.S_IWRITE) and (parent_imode & stat.S_IEXEC):
            set_permissions = True
            os.chmod(parent, parent_imode | stat.S_IEXEC | stat.S_IWRITE)

        to_raise = None

        try:
            func(path)
        except Exception as e:
            to_raise = e

        # Put it back! Must do this even if we fail the func!
        if set_permissions:
            os.chmod(parent, parent_imode)

        if to_raise:
            raise e

    def delete(self, path: Path):
        complete_path = self._resolved_path_store(path)

        if os.path.exists(complete_path):
            try:
                os.rmdir(complete_path)
            except NotADirectoryError:
                # It's not a directory. Delete it.
                os.remove(complete_path)
            except OSError:
                # Directory is not empty. Delete it and all its contents.
                # Commenting out: currently not a warning
                logger.info(
                    f"Directory {complete_path} is not empty. Deleting all contents"
                )
                shutil.rmtree(complete_path, onerror=self.remove_readonly_store)

        # Check if the parent is empty. We don't want to leave dregs!
        if os.path.exists(complete_path.parent):
            try:
                resolved_path = self._resolved_path_store(complete_path.parent)

                if any(resolved_path.iterdir()):
                    raise ValueError("Parent directory is not empty.")

                resolved_path.rmdir()
            except (ValueError, OSError):
                # The parent is not in the store area. We can't delete it, or
                # the folder that is the parent is not empty (there may be other
                # files under that directory that we haven't deleted yet).
                pass

        return

    def commit(self, staging_path: Path, store_path: Path):
        # Must import after initialization, unfortunately.
        from librarian_server.settings import server_settings

        need_ownership_changes = self.own_after_commit or self.readonly_after_commit

        resolved_path_staging = self._resolved_path_staging(staging_path)
        resolved_path_store = self._resolved_path_store(store_path)

        if not need_ownership_changes:
            # We can just move the file.
            shutil.move(
                resolved_path_staging,
                resolved_path_store,
            )

            return
        else:
            # We need to copy the file and then set the permissions. Copying can inherrently
            # introduce issues with corruption, so we need to be careful and make sure
            # we have a good copy.
            retries = 0
            copy_success = False

            original_checksum = get_checksum_from_path(
                resolved_path_staging, threads=server_settings.checksum_threads
            )

            while not copy_success and retries < self.max_copy_retries:
                if resolved_path_staging.is_dir():
                    shutil.copytree(resolved_path_staging, resolved_path_store)
                else:
                    shutil.copy2(resolved_path_staging, resolved_path_store)

                new_checksum = get_checksum_from_path(
                    resolved_path_store, threads=server_settings.checksum_threads
                )

                copy_success = compare_checksums(original_checksum, new_checksum)
                retries += 1

            # Commented out: somehow causing corruption?
            # if copy_success:
            #     # We need to clean up the files we just copied from; this is a 'move'
            #     # operation!
            #     try:
            #         os.rmdir(resolved_path_staging)
            #     except NotADirectoryError:
            #         # It's not a directory. Delete it.
            #         os.remove(resolved_path_staging)
            #     except OSError:
            #         # Directory is not empty. Delete it and all its contents.
            #         shutil.rmtree(resolved_path_staging)
            if not copy_success:
                # We need to clean up
                self.delete(store_path)

                logger.error(
                    "Could not copy {} to {} after {} attempts.",
                    resolved_path_staging,
                    resolved_path_store,
                    retries,
                )

                raise ValueError(
                    f"Could not copy {resolved_path_staging} to {resolved_path_store} "
                    f"after {retries} attempts."
                )

        try:
            # Set permissions and ownership.
            def set_for_file(file: Path):
                if self.own_after_commit:
                    shutil.chown(file, user=os.getuid(), group=os.getgid())

                if self.readonly_after_commit:
                    if file.is_dir():
                        os.chmod(
                            file,
                            stat.S_IREAD
                            | stat.S_IRGRP
                            | stat.S_IROTH
                            | stat.S_IXUSR
                            | stat.S_IXGRP
                            | stat.S_IXOTH,
                        )
                    else:
                        os.chmod(file, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

            # Set for the top-level file.
            set_for_file(resolved_path_store)

            # If this is a directory, walk.
            if resolved_path_store.is_dir():
                for root, dirs, files in os.walk(resolved_path_store):
                    for x in dirs + files:
                        set_for_file(Path(root) / x)
        except ValueError:
            logger.error(
                "Can not set permissions on {}",
                resolved_path_store,
            )
            raise PermissionError(f"Could not set permissions on {resolved_path_store}")

        return

    def store(self, path: Path) -> Path:
        # First, check if the file already exists, and it's within our store
        resolved_path = self._resolved_path_store(path)

        if resolved_path.exists():
            raise FileExistsError(f"File {path} already exists on store.")

        # Now create any directory structure that is required to store the file.
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

        return resolved_path

    def path_info(self, path: Path, hash_function: str = "xxh3") -> PathInfo:
        # Must import after initialization, unfortunately.
        from librarian_server.settings import server_settings

        # Promote path to object if required
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Path {path} does not exist")

        return PathInfo(
            # Use the old functions for consistency.
            path=path,
            filetype=get_type_from_path(str(path)),
            checksum=get_checksum_from_path(
                path,
                hash_function=hash_function,
                threads=server_settings.checksum_threads,
            ),
            size=get_size_from_path(path),
        )

    def can_transfer(self, using: CoreTransferManager):
        return using.valid

    def transfer_out(
        self, store_path: Path, destination_path: Path, using: CoreTransferManager
    ) -> bool:
        # First, check if the file exists on the store.
        resolved_store_path = self._resolved_path_store(store_path)

        if not resolved_store_path.exists():
            raise FileNotFoundError(f"File {store_path} does not exist on store.")

        if using.valid:
            # We can transfer it out!
            return using.transfer(resolved_store_path, destination_path)

        return False
