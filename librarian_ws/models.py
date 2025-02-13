"""
Core models for the websocket interactions. Each interaction is modeled as a
class built on WSMessage() with a discriminated union for content. The authentication
is handled and WSMessage() is converted to AuthenticatedWSMessage() (each underlying model
has a fixed requirement for authentication). That message can then be executed using
the shared `execute` method.
"""

import websockets

from .auth import ServerAuth
from .responses import Responses


class WSData:
    """
    Information about our connection to the client
    """

    open_connection: websockets.ServerConnection
    auth: ServerAuth

    def __init__(self, open_connection, auth):
        self.open_connection = open_connection
        self.auth = auth

    async def respond(self, response: Responses):
        await self.auth.send_encrypted(
            response=response, websocket=self.open_connection
        )
        return
