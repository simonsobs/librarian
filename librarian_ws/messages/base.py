import abc
from typing import Literal

from pydantic import BaseModel

ALLOWED_MESSAGE_TYPES = [
    "ping",
    "checksum",
]

AllowedMessageType = Literal[*ALLOWED_MESSAGE_TYPES]


class BaseMessage(BaseModel, abc.ABC):
    message_type: AllowedMessageType

    @abc.abstractmethod
    async def handle(self, data: "WSData"):
        return
