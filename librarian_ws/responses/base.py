import abc
from typing import Literal

from pydantic import BaseModel

ALLOWED_RESPONSE_TYPES = ["ping", "checksum"]

AllowedResponseType = Literal[*ALLOWED_RESPONSE_TYPES]


class BaseResponse(BaseModel, abc.ABC):
    response_type: AllowedResponseType
