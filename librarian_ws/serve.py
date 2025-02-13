"""
Serve the websocket server.
"""

import asyncio

from loguru import logger
from websockets.asyncio.server import serve

from .auth import ServerAuth
from .models import WSData

# Global set of connections currently being used
CONNECTIONS: dict[str, WSData] = {}


async def handler(websocket):
    """
    Serving
    """
    logger.info(
        "Opening new connection: id={id}, local={local}, remote={remote}",
        id=websocket.id,
        local=websocket.local_address,
        remote=websocket.remote_address,
    )
    data = WSData(open_connection=websocket, auth=ServerAuth(session_id=websocket.id))

    # Start with the public key exchange.
    logger.info("Attempting key generation for id={}", websocket.id)
    data.auth.generate_keys()
    logger.info(
        "Key generation successful, attmepting key ecchange for id={}", websocket.id
    )
    await data.auth.public_key_exchange(websocket)
    logger.info("Key exchange successful for id={}", websocket.id)

    CONNECTIONS[data.auth.session_id] = data

    try:
        async for message in data.open_connection:
            message_size = len(message)
            logger.info(
                "Recieved message for id={} of size={}; attempting decryption",
                websocket.id,
                message_size,
            )
            parsed_message = data.auth.decrypt(message)
            logger.debug(
                "Decryption for id={} and size={} successful; attempting authentication",
                websocket.id,
                message_size,
            )
            authenticated_data = await parsed_message.authenticate()
            logger.debug(
                "Authentication for id={} and size={} successful; user={}; attempting to handle message of type={}",
                websocket.id,
                message_size,
                authenticated_data.username,
                authenticated_data.message.message_type,
            )
            await authenticated_data.message.handle(data)
            logger.info(
                "Message succesfully handled for id={}, user={}, type={}",
                websocket.id,
                authenticated_data.username,
                authenticated_data.message.message_type,
            )
    finally:
        logger.info("Closing connection", websocket.id)
        CONNECTIONS.pop(data.auth.session_id)
        del data


async def core(host: str = "localhost", port: int = 2993):
    async with serve(handler, host, port):
        await asyncio.get_event_loop().create_future()


def main():
    asyncio.run(core())
