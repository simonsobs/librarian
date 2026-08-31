# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test that a 503 from validate/file becomes a distinct exception.

When a librarian cannot reach one of its downstreams it answers 503 rather
than returning a partial list. The client must turn that into
LibrarianDownstreamUnavailableError, so callers can tell "I could not check"
apart from "there are no remote copies".

"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from hera_librarian.client import LibrarianClient
from hera_librarian.exceptions import (
    LibrarianDownstreamUnavailableError,
    LibrarianHTTPError,
)

REASON = "Unable to contact downstream librarian downstream_a."
REMEDY = "Retry once the downstream librarian is reachable."


def _make_handler(status_code, body):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.dumps(body).encode()
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    return Handler


@pytest.fixture
def failing_server(request):
    """A server that answers every POST with a given status and body."""

    status_code, body = request.param

    server = HTTPServer(("127.0.0.1", 0), _make_handler(status_code, body))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server.server_address[1]

    server.shutdown()
    server.server_close()


def client_for(port):
    return LibrarianClient(
        host="http://127.0.0.1",
        port=port,
        user="user",
        password="password",
        request_timeout_seconds=5,
    )


@pytest.mark.parametrize(
    "failing_server",
    [(503, {"reason": REASON, "suggested_remedy": REMEDY})],
    indirect=True,
)
def test_503_becomes_downstream_unavailable(failing_server):
    """A 503 must not surface as a generic HTTP error."""

    client = client_for(failing_server)

    with pytest.raises(LibrarianDownstreamUnavailableError) as excinfo:
        client.validate_file("example_file.txt")

    assert "downstream_a" in excinfo.value.reason
    assert excinfo.value.suggested_remedy == REMEDY

    return


@pytest.mark.parametrize(
    "failing_server",
    [(400, {"reason": "no such file", "suggested_remedy": "check the name"})],
    indirect=True,
)
def test_other_errors_are_unchanged(failing_server):
    """Other failures must keep raising LibrarianHTTPError. This is the
    control: it shows we narrowed the new behaviour to 503 only."""

    client = client_for(failing_server)

    with pytest.raises(LibrarianHTTPError):
        client.validate_file("example_file.txt")

    return
