"""
Authentication flows for the websocket server. Authentication occurs in the
following manner:

- Server sends unencrypted session ID.
- Client responds with public key.
- Server send with public key.
- Begin encrypted exchange.

Further, we then check whether the username and password (required in each client
message to server, and encrypted in-flight) are checked against the contents of
our database.

Information exchange is provided through individual messages containing a:

- 16 byte header (UUID) identifying the client ID
- An encrypted blob

This encrypted blob is always one of WSMessage or WSResponse. These can
be decoded using the `decrypt` methods on the `ClientAuth` and `ServerAuth`
objects.
"""

import datetime
import uuid

import cryptography.hazmat.primitives.asymmetric.rsa as crypt_rsa
import websockets
from argon2.exceptions import VerifyMismatchError
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from pydantic import BaseModel

from hera_librarian.authlevel import AuthLevel
from librarian_ws.messages import Messages, WSMessage
from librarian_ws.responses import Responses, WSResponse

PADDING = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None
)

REQUIRED_AUTHENTICATION_LEVELS = {"ping": None, "checksum": AuthLevel.READAPPEND}


class UnauthorizedError(Exception):
    pass


class ClientAuth:
    """
    Client-side authentication object. Used for the key pair exchange
    and for sending messages to the server.
    """

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
        self, message: Messages, websocket: websockets.ClientConnection
    ):
        print(message)
        await websocket.send(self.encrypt(message=message))

    async def recv_encrypted(self, websocket: websockets.ClientConnection) -> Responses:
        raw = await websocket.recv()
        return self.decrypt(content=raw).response

    def encrypt(self, message: Messages) -> bytes:
        """
        Encrypt a pydantic model using the public key.
        """
        data = self.session_id + self.server_public_key.encrypt(
            WSMessage(
                username="NONE",
                password="NONE",
                time=datetime.datetime.now(tz=datetime.timezone.utc),
                # For some reason this will _not_ let me use the underlying pydantic
                # models and requires serialization down to a dictionary before being
                # de-serialized back to a model.
                message=message.model_dump(),
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

    def __init__(self, session_id: uuid.uuid4):
        """
        Set up the server authentication object for this client. Note
        that session_id should be provided from the websocket itself
        (websocket.id).
        """
        self.session_id = session_id.bytes

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
        self, response: Responses, websocket: websockets.ServerConnection
    ):
        await websocket.send(self.encrypt(response=response))

    async def recv_encrypted(self, websocket: websockets.ServerConnection) -> Messages:
        raw = await websocket.recv()
        message = self.decrypt(raw)
        return (await message.authenticate()).message

    def encrypt(self, response: Responses) -> bytes:
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
