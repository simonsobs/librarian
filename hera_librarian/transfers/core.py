"""
Core transfer manager (prototype)
"""

import abc

from pydantic import BaseModel


class CoreTransferManager(BaseModel, abc.ABC):
    @abc.abstractmethod
    def transfer(self, local_path: str, remote_path: str):
        """
        Transfer a file from the local machine to the store.

        Parameters
        ----------
        local_path : str
            Path to the local file to upload.
        store_path : str
            Path to store file at on destination host.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def valid(self) -> bool:
        """
        Whether or not this transfer manager is valid for the
        current system we are running on.
        """
        raise NotImplementedError
