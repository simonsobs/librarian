"""
Tests for admin endpoints.
"""

import hashlib
import shutil

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.models.admin import (
    AdminCreateFileRequest,
    AdminCreateFileResponse,
    AdminRequestFailedResponse,
    AdminVerifyFileRequest,
)
from hera_librarian.utils import get_md5_from_path, get_size_from_path


def test_add_file(test_client, test_server, garbage_file, test_orm):
    """
    Tests that we can add a file with no row in database.
    """

    # First, create the file in the store.
    setup = test_server[2]

    store = setup.store_directory

    full_path = store / "test_upload_without_uploading.txt"

    # Create the file in the store.
    shutil.copy2(garbage_file, full_path)

    request = AdminCreateFileRequest(
        name="test_upload_without_uploading.txt",
        create_time=garbage_file.stat().st_ctime,
        size=garbage_file.stat().st_size,
        checksum=get_md5_from_path(full_path),
        uploader="test",
        source="test",
        path=str(full_path),
        store_name="local_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = AdminCreateFileResponse.model_validate_json(response.content)

    assert response.success

    # Now can check what happens if we upload the file again...

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 200

    response = AdminCreateFileResponse.model_validate_json(response.content)

    assert response.already_exists

    # Ok, now validate the actual db.

    get_session = test_server[1]

    with get_session() as session:
        file = session.get(test_orm.File, "test_upload_without_uploading.txt")

        assert file is not None

        instance = file.instances[0]

        assert instance is not None

        assert instance.path == str(full_path)
        assert instance.store.name == "local_store"


def test_add_flie_no_file_exists(test_client, test_server, test_orm):
    """
    Tests that we can't add a file if the file doesn't exist.
    """

    request = AdminCreateFileRequest(
        name="non_existent_file.txt",
        create_time=0,
        size=0,
        checksum="",
        uploader="test",
        source="test",
        path="/this/file/does/not/exist",
        store_name="local_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 400

    response = AdminRequestFailedResponse.model_validate_json(response.content)

    assert response.reason == "File /this/file/does/not/exist does not exist."
    assert (
        response.suggested_remedy
        == "Create the file first, or make sure that you are using a local store."
    )


def test_add_file_no_store_exists(test_client):
    """
    Tests the case where the store does not exist and we try to add a file.
    """

    request = AdminCreateFileRequest(
        name="non_existent_file.txt",
        create_time=0,
        size=0,
        checksum="",
        uploader="test",
        source="test",
        path="/this/file/does/not/exist",
        store_name="not_a_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/add_file", content=request.model_dump_json()
    )

    assert response.status_code == 400

    response = AdminRequestFailedResponse.model_validate_json(response.content)

    assert response.reason == "Store not_a_store does not exist."


def test_verify_file_success(test_client, test_server, garbage_file, test_orm):
    """
    Tests that a file's properties match the database record.
    """
    setup = test_server[2]
    store = setup.store_directory
    full_path = store / "test_file_to_verify.txt"
    # Create the file in the store
    shutil.copy2(garbage_file, full_path)

    # Get the session and ORM models from the test_server fixture
    session = test_server[1]()

    # Create or find a store in the database
    db_store = (
        session.query(test_orm.StoreMetadata).filter_by(name="local_store").first()
    )
    if not db_store:
        # Create a new store if not found (adjust attributes as necessary)
        db_store = test_orm.StoreMetadata(
            name="local_store", description="A local store for testing"
        )
        session.add(db_store)
        session.commit()

    # Create file and instance records in the database
    file_data = open(full_path, "rb").read()
    db_file = test_orm.File.new_file(
        filename="test_file_to_verify.txt",
        size=len(file_data),
        checksum=hashlib.md5(file_data).hexdigest(),
        uploader="test_uploader",  # Adjust as necessary
        source="test_source",  # Adjust as necessary
    )
    instance = test_orm.Instance.new_instance(
        path=str(full_path),
        file=db_file,
        store=db_store,
        deletion_policy="ALLOWED",  # Adjust as necessary
    )
    session.add_all([db_file, instance])
    session.commit()

    # Assume the file has been added to the database already; here we simulate the verification request
    verify_request = AdminVerifyFileRequest(
        name="test_file_to_verify.txt",
        size=get_size_from_path(full_path),
        checksum=get_md5_from_path(full_path),
        store_name="local_store",
    )

    response = test_client.post_with_auth(
        "/api/v2/admin/verify_file", json=verify_request.dict()
    )

    assert response.status_code == 200

    # Clean up: Delete the added records and file
    session.delete(instance)
    session.delete(db_file)
    session.commit()
    full_path.unlink()  # Remove the file from the filesystem

    session.close()
    response_data = response.json()
    assert response_data["verified"] == True
    assert isinstance(response_data["checksums_and_sizes"], list)
    assert len(response_data["checksums_and_sizes"]) > 0
    assert "checksum" in response_data["checksums_and_sizes"][0]
    assert "size" in response_data["checksums_and_sizes"][0]
    assert "store_id" in response_data["checksums_and_sizes"][0]


def test_verify_file_failure(test_client, test_server, test_orm):
    """
    Tests that verification fails when file properties do not match.
    """
    # Assume a file "mismatched_file.txt" exists in the database but with different properties
    request = {
        "name": "mismatched_file.txt",
        "size": 123,  # Intentionally incorrect size
        "checksum": "wrongchecksum",  # Intentionally incorrect checksum
        "store_name": "local_store",
    }

    response = test_client.post_with_auth("/api/v2/admin/verify_file", json=request)

    assert response.status_code == 400
    assert response.json() == {"detail": "File not found."}
