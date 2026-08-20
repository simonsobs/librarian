"""
Exceptions for the hera_librarian client library.
"""


class LibrarianHTTPError(Exception):
    def __init__(self, url, status_code, reason, suggested_remedy, full_response=None):
        super(LibrarianHTTPError, self).__init__(
            f"HTTP request to {url} failed with status code {status_code} and reason {reason}."
        )
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.suggested_remedy = suggested_remedy
        self.full_response = full_response


class LibrarianTimeoutError(Exception):
    def __init__(self, url):
        super(LibrarianTimeoutError, self).__init__(
            f"HTTP request to {url} timed out or took too many retries."
        )
        self.url = url


class LibrarianError(Exception):
    def __init__(self, message):
        super(LibrarianError, self).__init__(message)


class LibrarianClientRemovedFunctionality(Exception):
    def __Init__(self, name, message):
        super(LibrarianClientRemovedFunctionality, self).__init__(
            f"{name} is no longer avaialble in Librarian v2.0. {message}"
        )


class LibrarianDownstreamUnavailableError(Exception):
    """
    Raised when a librarian could not contact one of its downstream librarians
    while answering our request, so its answer would have been incomplete.

    This is distinct from a librarian telling us that a file has no remote
    copies: here, we simply do not know.
    """

    def __init__(self, url, reason, suggested_remedy=None):
        super().__init__(
            f"The librarian at {url} could not reach a downstream librarian, so "
            f"its answer is incomplete. {reason}"
        )
        self.url = url
        self.reason = reason
        self.suggested_remedy = suggested_remedy
