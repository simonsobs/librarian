"""
Asynchronous database connection and operations.
"""

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from librarian_server.settings import server_settings

logger.info("Starting async database engine")

engine = create_async_engine(
    server_settings.sqlalchemy_async_database_uri,
    echo=server_settings.debug,
)


async def get_session() -> AsyncSession:
    """
    Returns a new database session.
    """
    async_session = async_sessionmaker(bind=engine, expire_on_commit=False)
    async with async_session() as session:
        yield session
