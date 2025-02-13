from typing import Literal

from librarian_ws.responses.ping import PingResponse

from .base import BaseMessage


class PingMessage(BaseMessage):
    message_type: Literal["ping"] = "ping"

    async def handle(self, data: "WSData"):
        await data.respond(response=PingResponse())
        return
