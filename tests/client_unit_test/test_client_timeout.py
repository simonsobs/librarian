# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test timeout handling in hera_librarian/client.py

Covers two things:

- that network failures are translated into LibrarianTimeoutError, for both
  kinds of timeout that requests can raise
- that timeout settings survive the trip from ClientInfo through from_info()

"""

import socket
import threading
import time

import pytest

from hera_librarian.client import LibrarianClient
from hera_librarian.exceptions import LibrarianTimeoutError
from hera_librarian.settings import ClientInfo

# Short enough to keep the suite fast, long enough not to be flaky.
TEST_TIMEOUT_SECONDS = 2


@pytest.fixture
def stalling_port():
    """A local port that accepts connections and then never responds.

    Simulates a librarian whose process is wedged: the TCP handshake
    succeeds, so this is not a connection failure, but no HTTP response
    ever arrives.
    """

    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(5)

    held = []
    stop = threading.Event()

    def accept_and_stall():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
                held.append(conn)
            except OSError:
                return

    thread = threading.Thread(target=accept_and_stall, daemon=True)
    thread.start()

    yield listener.getsockname()[1]

    stop.set()
    listener.close()
    for conn in held:
        conn.close()


@pytest.fixture
def refusing_port():
    """A local port with nothing listening on it at all."""

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    return port


def client_for(port):
    """A client pointed at a local port, with an explicit short timeout."""

    return LibrarianClient(
        host="http://127.0.0.1",
        port=port,
        user="user",
        password="password",
        request_timeout_seconds=TEST_TIMEOUT_SECONDS,
    )


def test_connection_refused_raises_librarian_timeout(refusing_port):
    """A refused connection should surface as LibrarianTimeoutError."""

    client = client_for(refusing_port)

    with pytest.raises(LibrarianTimeoutError):
        client.ping()

    return


def test_read_timeout_raises_librarian_timeout(stalling_port):
    """A server that accepts and then stalls should also surface as
    LibrarianTimeoutError, not as a raw requests exception."""

    client = client_for(stalling_port)

    with pytest.raises(LibrarianTimeoutError):
        client.ping()

    return


def test_from_info_passes_request_timeout():
    """request_timeout_seconds set in config should reach the client."""

    info = ClientInfo(
        user="user",
        port=1234,
        host="http://127.0.0.1",
        password="password",
        request_timeout_seconds=60,
    )

    client = LibrarianClient.from_info(info)

    assert client.request_timeout_seconds == 60

    return


def test_from_info_passes_checksum_threads():
    """checksum_threads set in config should reach the client."""

    info = ClientInfo(
        user="user",
        port=1234,
        host="http://127.0.0.1",
        password="password",
        checksum_threads=8,
    )

    client = LibrarianClient.from_info(info)

    assert client.checksum_threads == 8

    return
