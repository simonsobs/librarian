"""
Messages from server to client. These are encoded as pydantic
models.
"""

import datetime

from pydantic import BaseModel, Field

from .base import BaseResponse
from .ping import PingResponse

Responses = PingResponse


class WSResponse(BaseModel):
    """
    A response from the server to the client.
    """

    time: datetime.datetime
    response: Responses = Field(discriminator="response_type")
