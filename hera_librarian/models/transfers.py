"""
Pydantic models for the remote transfer update endpoints
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, RootModel


class LibrarianTransfersStatusRequest(BaseModel):
    """
    A request to report the transfer status of a librarian, either
    to enable or disable outbound transfers.
    """
    "The name of the librarian to change the transfer status of."
    librarian_name: str

class LibrarianTransfersStatusResponse(BaseModel):
    """
    A response to a user transfer status request.
    """
    "The name of the librarian that was changed."
    librarian_name: str

    "Whether the librarian has outbound transfers enabled."
    transfers_enabled: bool

class LocalLibrarianTransfersStatusRequest(BaseModel):
    """
    A request to change the transfer status of a librarian, either
    to enable or disable inbound transfers.
    """
    "The name of the librarian to change the transfer status from."
    librarian_name: str
    "Whether to enable or disable outbound transfers."
    transfers_enabled: bool

class LibrarianTransfersUpdateRequest(BaseModel):
    """
    A request to change the transfer status of a librarian, either
    to enable or disable outbound transfers.
    """

    "The name of the librarian to change the transfer status of."
    librarian_name: str

    "Whether to enable or disable outbound transfers."
    transfers_enabled: bool

class LibrarianTransfersUpdateResponse(BaseModel):
    """
    A response to a user change request.
    """

    "The name of the librarian that was changed."
    librarian_name: str

    "Whether the librarian has outbound transfers enabled."
    transfers_enabled: bool

class LibrarianTransfersFailedResponse(BaseModel):
    reason: str
    "The reason why the search failed."

    suggested_remedy: str
    "A suggested remedy for the failure."