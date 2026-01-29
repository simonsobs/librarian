"""
Integration test for enabling and disabling transfers to a librarian.
"""

import random
from pathlib import Path

import pytest

from hera_librarian.exceptions import LibrarianError

# What I have to do:
# 1. Create two servers (done via fixtures)
# 2. Register each other as librarians (done)
# 3. Authenticate the clients as admin (done)
# 4. Disable transfers on one of them via the API (to test the API)
# 5. Check the database to ensure the status is updated (to test the database)
# 6. Re-enable transfers on the other one via the API (to test the API)
# 7. Check the database to ensure the status is updated (to test the database)


def test_enable_disable_transfers(
    test_server_disable,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    tmp_path,
):
    # Register downstream and upstream librarians
    assert mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="admin:password",
        port=server.id,
    )
    assert admin_client.add_librarian(
        name="test_server",
        url="http://localhost",
        authenticator="admin:password",
        port=test_server_disable[2].id,
        check_connection=False,
    )

    # Disable transfers for test_server using the API
    result = admin_client.set_librarian_status(
        librarian_name="test_server",
        transfers_enabled=False,
    )
    assert result is False

    # Verify in the database
    with librarian_database_session_maker() as session:
        test_librarian = (
            session.query(test_orm.Librarian).filter_by(name="test_server").one()
        )
        assert test_librarian.transfers_enabled is False

    # # Re-enable transfers for live_server using the API
    result = admin_client.set_librarian_status(
        librarian_name="test_server",
        transfers_enabled=True,
    )
    assert result is True

    # # Verify in the database
    with librarian_database_session_maker() as session:
        test_librarian = (
            session.query(test_orm.Librarian).filter_by(name="test_server").one()
        )
        assert test_librarian.transfers_enabled is True

    # # Clean up
    assert mocked_admin_client.remove_librarian(name="live_server")
    assert admin_client.remove_librarian(name="test_server")
