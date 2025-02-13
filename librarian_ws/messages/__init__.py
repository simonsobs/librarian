"""
Messages from client to server. These are encoded as pydantic
models.
"""

import datetime
from typing import Union

from pydantic import BaseModel, Field

from hera_librarian.authlevel import AuthLevel

from .base import BaseMessage
from .checksum import ChecksumMessage
from .ping import PingMessage

Messages = Union[PingMessage, ChecksumMessage]


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
        # ThESE VALUES ARE NOW ACTUALLY DEINFED IN AUTH.py
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
