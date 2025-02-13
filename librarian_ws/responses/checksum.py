from typing import Literal

from .base import BaseResponse


class ChecksumResponse(BaseResponse):
    response_type: Literal["checksum"] = "checksum"
