from typing import Literal

from .base import BaseResponse


class PingResponse(BaseResponse):
    response_type: Literal["ping"] = "ping"
