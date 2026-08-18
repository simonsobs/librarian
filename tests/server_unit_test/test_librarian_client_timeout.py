# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test that Librarian.client() applies the configured request timeout.

Without a timeout, a call to a librarian that accepts the connection and then
stops responding will wait forever, which stalls the validate fan-out.

Note that librarian_server imports are deliberately made inside the functions
below. server_settings is a lazily-loaded singleton, and importing it at module
scope forces it to load during test collection, before the fixtures that
configure it have run.

"""

from datetime import datetime, timezone

from cryptography.fernet import Fernet


def _librarian():
    """An in-memory Librarian row. Not attached to a session."""

    from librarian_server.encryption import encrypt_string
    from librarian_server.orm.librarian import Librarian

    return Librarian(
        name="downstream",
        url="http://localhost",
        port=1234,
        authenticator=encrypt_string("user:password"),
        last_seen=datetime.now(timezone.utc),
        last_heard=datetime.now(timezone.utc),
    )


def test_client_uses_configured_timeout(monkeypatch):
    """The server setting should reach the client we build for a remote
    librarian."""

    from librarian_server.settings import server_settings

    monkeypatch.setattr(
        server_settings, "encryption_key", Fernet.generate_key().decode()
    )
    monkeypatch.setattr(server_settings, "librarian_request_timeout_seconds", 42)

    assert _librarian().client().request_timeout_seconds == 42

    return


def test_client_timeout_defaults_to_a_real_value(monkeypatch):
    """Out of the box the timeout must not be None, which requests treats as
    "wait forever"."""

    from librarian_server.settings import server_settings

    monkeypatch.setattr(
        server_settings, "encryption_key", Fernet.generate_key().decode()
    )

    assert _librarian().client().request_timeout_seconds is not None

    return
