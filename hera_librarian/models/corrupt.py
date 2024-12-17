"""
Models for the corruption fixing endpoints.
"""

from pydantic import BaseModel


class CorruptionPreparationRequest(BaseModel):
    file_name: str
    librarian_name: str


class CorruptionPreparationResponse(BaseModel):
    ready: bool


class CorruptionResendRequest(BaseModel):
    librarian_name: str
    file_name: str


class CorruptionResendResponse(BaseModel):
    success: bool
    destination_transfer_id: int
