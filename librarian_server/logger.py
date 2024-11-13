"""
Logging setup. Use this as 'from logger import log'
"""

import inspect

import loguru
import requests
from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity

from .settings import server_settings

log_settings = server_settings.log_settings

log_settings.setup_logs()
loguru.logger.debug("Logging set up.")

log = loguru.logger


def post_text_event_to_slack(text: str) -> None:
    loguru.logger.info(text)

    return


def post_error_to_slack(error: "Error") -> None:
    return


def log_to_database(
    severity: ErrorSeverity, category: ErrorCategory, message: str, session: Session
) -> None:
    """
    Log an error to the database.

    Parameters
    ----------

    severity : ErrorSeverity
        The severity of this error.
    category : ErrorCategory
        The category of this error.
    message : str
        The message describing this error.
    session : Session
        The database session to use.

    Notes
    -----

    Automatically stores the above frame's file name, function, and line number in
    the 'caller' field of the error.
    """

    # Convert severity to log level

    use_func = {
        ErrorSeverity.CRITICAL: loguru.logger.error,
        ErrorSeverity.ERROR: loguru.logger.error,
        ErrorSeverity.WARNING: loguru.logger.warning,
        ErrorSeverity.INFO: loguru.logger.info,
    }[severity]

    use_func(message)
