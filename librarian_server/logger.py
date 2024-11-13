"""
Logging setup. Use this as 'from logger import log'
"""

import inspect

import loguru as log
import requests
from sqlalchemy.orm import Session

from hera_librarian.errors import ErrorCategory, ErrorSeverity

from .settings import server_settings

log_settings = server_settings.log_settings

log_settings.setup_logs()
log.debug("Logging set up.")


def post_text_event_to_slack(text: str) -> None:
    log.info(text)

    return


def post_error_to_slack(error: "Error") -> None:
    if not server_settings.slack_webhook_enable:
        return

    if error.severity not in server_settings.slack_webhook_post_error_severity:
        return

    if error.category not in server_settings.slack_webhook_post_error_category:
        return

    requests.post(
        server_settings.slack_webhook_url,
        json={
            "username": server_settings.displayed_site_name,
            "icon_emoji": ":ledger:",
            "text": (
                f"*New Librarian Error at {server_settings.name}*\n"
                f"> _Error Severity_: {error.severity.name}\n"
                f"> _Error Category_: {error.category.name}\n"
                f"> _Error Message_: {error.message}\n"
                f"> _Error ID_: {error.id}\n"
                f"> _Error Raised Time_: {error.raised_time}\n"
                f"`{error.caller}`"
            ),
        },
    )


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
        ErrorSeverity.CRITICAL: log.error,
        ErrorSeverity.ERROR: log.error,
        ErrorSeverity.WARNING: log.warning,
        ErrorSeverity.INFO: log.info,
    }[severity]

    use_func(message)
