"""
Core models for the websocket interactions. Each interaction is modeled as a
class built on WSMessage() with a discriminated union for content. The authentication
is handled and WSMessage() is converted to AuthenticatedWSMessage() (each underlying model
has a fixed requirement for authentication). That message can then be executed using
the shared `execute` method.
"""

import abc
import asyncio
import datetime
import json
import uuid
from typing import Literal, Union

import cryptography.hazmat.primitives.asymmetric.rsa as crypt_rsa
import websockets

# from librarian_ws.database import get_session
from argon2.exceptions import VerifyMismatchError
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from pydantic import BaseModel, Field
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from hera_librarian.authlevel import AuthLevel
from librarian_server import orm

# --- Authentication

PADDING = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None
)


class UnauthorizedError(Exception):
    pass


class HandshakeMessage(BaseModel):
    username: str


class HandshakeResponse(BaseModel):
    session_id: str
    public_key: str


class ClientAuth:
    private_key: crypt_rsa.RSAPrivateKey
    public_key: crypt_rsa.RSAPublicKey

    server_public_key: crypt_rsa.RSAPublicKey
    session_id: bytes

    def generate_keys(self):
        self.private_key = crypt_rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        self.public_key = self.private_key.public_key()

    async def public_key_exchange(self, websocket: websockets.ClientConnection):
        """
        Given a fresh websocket, perform the public key and session ID exchange.
        """
        # Ping pong works as follows:
        # Server sends unencrypted session ID.
        # Client responds with public key.
        # Server send with public key.
        # Begin encrypted exchange.

        self.session_id = await websocket.recv()
        await websocket.send(
            self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
        raw_key = await websocket.recv()
        self.server_public_key = serialization.load_pem_public_key(data=raw_key)

        # Done, ready for encrypted communication.

        return

    async def send_encrypted(
        self, message: "BaseMessage", websocket: websockets.ClientConnection
    ):
        await websocket.send(self.encrypt(message=message))

    async def recv_encrypted(
        self, websocket: websockets.ClientConnection
    ) -> "BaseResponse":
        raw = await websocket.recv()
        return self.decrypt(content=raw).response

    def encrypt(self, message: BaseModel) -> bytes:
        """
        Encrypt a pydantic model using the public key.
        """
        data = self.session_id + self.server_public_key.encrypt(
            WSMessage(
                username="NONE",
                password="NONE",
                time=datetime.datetime.now(tz=datetime.timezone.utc),
                message=message,
            )
            .model_dump_json()
            .encode("utf-8"),
            padding=PADDING,
        )

        return data

    def decrypt(self, content: bytes) -> "WSResponse":
        """
        Attempt to decrypt a message using the public key.
        """
        # First 16 bytes are UUID.
        session_id = content[:16]
        assert self.session_id == session_id
        return WSResponse.model_validate_json(
            self.private_key.decrypt(content[16:], padding=PADDING).decode("utf-8")
        )


class ServerAuth:
    private_key: crypt_rsa.RSAPrivateKey
    public_key: crypt_rsa.RSAPublicKey

    client_public_key: crypt_rsa.RSAPublicKey
    session_id: bytes

    def generate_keys(self):
        self.private_key = crypt_rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        self.public_key = self.private_key.public_key()

    async def public_key_exchange(self, websocket: websockets.ServerConnection):
        """
        Given a fresh websocket, perform the public key and session ID exchange.
        """

        # Ping pong works as follows:
        # Server sends unencrypted session ID.
        # Client responds with public key.
        # Server send with public key.
        # Begin encrypted exchange.

        self.session_id = uuid.uuid4().bytes
        await websocket.send(self.session_id)
        raw_key = await websocket.recv()
        self.client_public_key = serialization.load_pem_public_key(data=raw_key)
        await websocket.send(
            self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )

        # Done, ready for encrypted communication.

        return

    async def send_encrypted(
        self, response: "BaseResponse", websocket: websockets.ServerConnection
    ):
        await websocket.send(self.encrypt(response=response))

    async def recv_encrypted(
        self, websocket: websockets.ServerConnection
    ) -> "BaseMessage":
        raw = await websocket.recv()
        message = self.decrypt(raw)
        return (await message.authenticate()).message

    def encrypt(self, response: "BaseResponse") -> bytes:
        data = self.session_id + self.client_public_key.encrypt(
            WSResponse(
                time=datetime.datetime.now(tz=datetime.timezone.utc), response=response
            )
            .model_dump_json()
            .encode("utf-8"),
            padding=PADDING,
        )

        return data

    def decrypt(self, content: bytes) -> "WSMessage":
        # First 16 bytes are UUID.
        session_id = content[:16]
        assert self.session_id == session_id
        return WSMessage.model_validate_json(
            self.private_key.decrypt(content[16:], padding=PADDING).decode("utf-8")
        )


# --- Underlying messages (from client to server).

ALLOWED_MESSAGE_TYPES = [
    "ping",
]

AllowedMessageType = Literal[*ALLOWED_MESSAGE_TYPES]


class BaseMessage(BaseModel, abc.ABC):
    message_type: AllowedMessageType
    required_auth_level: AuthLevel

    @abc.abstractmethod
    async def handle(self, data: "WSData"):
        return


class PingMessage(BaseMessage):
    message_type: Literal["ping"] = "ping"
    required_auth_level: AuthLevel = AuthLevel.NONE

    async def handle(self, data: "WSData"):
        await data.respond(response=PingResponse())
        return


Messages = PingMessage


# --- Underlying responses (from server to client).

ALLOWED_RESPONSE_TYPES = [
    "ping",
]

AllowedResponseType = Literal[*ALLOWED_RESPONSE_TYPES]


class BaseResponse(BaseModel, abc.ABC):
    response_type: AllowedResponseType


class PingResponse(BaseResponse):
    response_type: Literal["ping"] = "ping"


Responses = PingResponse

# --- Core


class WSClient(BaseModel):
    url: str
    username: str
    password: str

    def connect(self):
        return


class WSData:
    """
    Information about our connection to the client
    """

    open_connection: websockets.ServerConnection
    auth: ServerAuth

    def __init__(self, open_connection, auth):
        self.open_connection = open_connection
        self.auth = auth

    async def respond(self, response: BaseResponse):
        await self.auth.send_encrypted(
            response=response, websocket=self.open_connection
        )
        return


class WSMessage(BaseModel):
    """
    A base websocket message.
    """

    username: str
    password: str
    time: datetime.datetime
    message: Messages = Field(discriminator="message_type")

    async def authenticate(self) -> "AuthenticatedWSMessage":
        """
        Raises
        ------
        UnauthorizedError
            If the user does not exist, has the wrong password, or
            does not meet the requirements for authenticating with this message.
        """
        return AuthenticatedWSMessage(
            username=self.username,
            time=self.time,
            auth_level=AuthLevel.ADMIN,
            message=self.message,
        )
        async with get_session() as session:
            user = session.get(orm.User, self.username)

            if user is None:
                raise UnauthorizedError

            try:
                user.check_password(self.password)
            except VerifyMismatchError:
                raise UnauthorizedError

        # Now we have the user and its auth level. We can check if it is high enough.
        if user.permission.value < self.message.required_auth_level.value:
            raise UnauthorizedError

        return AuthenticatedWSMessage(
            username=self.username,
            time=self.time,
            auth_level=user.permission,
            message=self.message,
        )


class AuthenticatedWSMessage(BaseModel):
    """
    A websocket message that has passed the minimum requirement
    for authentication level and user.
    """

    username: str
    time: datetime.datetime
    auth_level: AuthLevel
    message: Messages = Field(discriminator="message_type")


class WSResponse(BaseModel):
    """
    A response from the server to the client.
    """

    time: datetime.datetime
    response: Responses = Field(discriminator="response_type")


# --- Server setup

CONNECTIONS: dict[str, WSData] = {}


async def handler(websocket):
    """
    Serving
    """
    print("Opening connection")
    data = WSData(open_connection=websocket, auth=ServerAuth())

    # Start with the public key exchange.
    data.auth.generate_keys()
    await data.auth.public_key_exchange(websocket)

    print(data.auth)

    CONNECTIONS[data.auth.session_id] = data

    try:
        async for message in data.open_connection:
            # Decode it!
            parsed_message = data.auth.decrypt(message)
            authenticated_data = await parsed_message.authenticate()
            print("Parsed request:", authenticated_data, data.auth.session_id)
            await authenticated_data.message.handle(data)
    finally:
        CONNECTIONS.pop(data.auth.session_id)
        print("Closing connection", data.auth.session_id)
        del data


PORT = 2993


async def main():
    async with serve(handler, "localhost", PORT):
        await asyncio.get_event_loop().create_future()


async def run():
    uri = f"ws://localhost:{PORT}"
    async with connect(uri) as websocket:
        # Start with the public key exchange.
        auth = ClientAuth()

        auth.generate_keys()
        await auth.public_key_exchange(websocket)

        print(auth)

        for _ in range(10):
            await auth.send_encrypted(message=PingMessage(), websocket=websocket)
            response = await auth.recv_encrypted(websocket)

            print(response)
            await asyncio.sleep(1)


if __name__ == "__main__":
    import sys

    if sys.argv[1] == "client":
        asyncio.run(run())

    if sys.argv[1] == "server":
        asyncio.run(main())
