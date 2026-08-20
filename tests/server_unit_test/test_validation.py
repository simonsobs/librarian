"""
Tests the endpoints in librarian_server/api/validate.py.
"""

from hera_librarian.models.validate import (
    FileValidationRequest,
    FileValidationResponse,
    FileValidationResponseItem,
)


def test_validate_file(test_server_with_valid_file, test_client):
    request = FileValidationRequest(file_name="example_file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = FileValidationResponse.model_validate_json(response.content).root

    assert len(response) == 1

    response = response[0]

    assert isinstance(response, FileValidationResponseItem)

    assert response.librarian == "test_server"

    assert response.computed_same_checksum

    # Modern checksums come with a hash function prefix
    assert (
        response.current_checksum.split(":")[-1]
        == response.original_checksum.split(":")[-1]
    )


def test_validate_file_invalid(
    test_server_with_invalid_file,
    test_client,
):
    request = FileValidationRequest(file_name="example_file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = FileValidationResponse.model_validate_json(response.content).root

    assert len(response) == 1

    response = response[0]

    assert isinstance(response, FileValidationResponseItem)

    assert response.librarian == "test_server"

    assert not response.computed_same_checksum

    # Modern checksums come with a hash function prefix
    assert (
        response.current_checksum.split(":")[-1]
        != response.original_checksum.split(":")[-1]
    )


def test_validate_file_not_found(test_server, test_client):
    request = FileValidationRequest(file_name="not-an-existing-file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 400


def test_validate_file_downstream_unreachable(
    test_server_with_valid_file, test_client, test_orm
):
    """A downstream we cannot contact must not be reported as holding no
    copies. The endpoint should refuse to answer instead."""

    import socket
    from datetime import datetime, timezone

    from librarian_server.encryption import encrypt_string

    # Pick a port with nothing listening on it.
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    dead_port = probe.getsockname()[1]
    probe.close()

    session = test_server_with_valid_file[1]()

    librarian = test_orm.Librarian(
        name="unreachable_librarian",
        url="http://localhost",
        port=dead_port,
        authenticator=encrypt_string("user:password"),
        last_seen=datetime.now(timezone.utc),
        last_heard=datetime.now(timezone.utc),
    )
    session.add(librarian)
    session.commit()

    remote_instance = test_orm.RemoteInstance(
        file_name="example_file.txt",
        store_id=1,
        librarian_id=librarian.id,
        copy_time=datetime.now(timezone.utc),
        sender="test_server",
    )
    session.add(remote_instance)
    session.commit()

    remote_instance_id = remote_instance.id
    librarian_id = librarian.id
    session.close()

    request = FileValidationRequest(file_name="example_file.txt")

    response = test_client.post_with_auth(
        "/api/v2/validate/file", content=request.model_dump_json()
    )

    assert response.status_code == 503
    assert "unreachable_librarian" in response.json()["reason"]

    # Clean up so we do not affect other tests.
    session = test_server_with_valid_file[1]()
    session.delete(session.get(test_orm.RemoteInstance, remote_instance_id))
    session.delete(session.get(test_orm.Librarian, librarian_id))
    session.commit()
    session.close()

    return
