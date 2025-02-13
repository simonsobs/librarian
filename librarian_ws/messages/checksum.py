from typing import Literal

from .base import BaseMessage


class ChecksumMessage(BaseMessage):
    message_type: Literal["checksum"] = "ping"

    async def handle(self, data: "WSData"):
        # Do the thing
        return
