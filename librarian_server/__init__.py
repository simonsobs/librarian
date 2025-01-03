"""
The Librarian v2.0 server.

We have moved from Flask to FastAPI to ensure that web requests can be performed
asynchronously, and that background tasks can work on any available ASGI server.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .settings import server_settings


@asynccontextmanager
async def slack_post_at_startup_shutdown(app: FastAPI):
    """
    Lifespan event that posts to the slack hook once
    the FastAPI server starts up and shuts down.
    """
    from loguru import logger

    logger.info("Librarian server starting up")
    yield
    logger.info("Librarian server shutting down")


def main() -> FastAPI:
    from loguru import logger

    logger.info("Starting Librarian v2.0 server.")
    logger.debug("Creating FastAPI app instance.")

    app = FastAPI(
        title=server_settings.displayed_site_name,
        description=server_settings.displayed_site_description,
        openapi_url="/api/v2/openapi.json" if server_settings.debug else None,
        lifespan=slack_post_at_startup_shutdown,
    )

    logger.debug("Adding API router.")

    from .api import (
        admin_router,
        checkin_router,
        clone_router,
        corrupt_router,
        error_router,
        ping_router,
        search_router,
        upload_router,
        users_router,
        validate_router,
    )

    app.include_router(upload_router)
    app.include_router(ping_router)
    app.include_router(clone_router)
    app.include_router(search_router)
    app.include_router(error_router)
    app.include_router(users_router)
    app.include_router(admin_router)
    app.include_router(checkin_router)
    app.include_router(validate_router)
    app.include_router(corrupt_router)

    return app
