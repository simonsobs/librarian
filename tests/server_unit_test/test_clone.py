"""
Unit tests for endpoints in librarian_server/api/clone.py.
"""

from hera_librarian.models.clone import (
    CloneInitiationRequest,
    CloneInitiationResponse,
    CloneOngoingRequest,
    CloneOngoingResponse,
    CloneCompleteRequest,
    CloneCompleteResponse,
    CloneFailedResponse,
    CloneFailResponse,
    CloneFailRequest,
)

def test_stage_negative_clone(client):
    """
    Tests that a negative upload size results in an error.
    """

    request = CloneInitiationRequest(
        destination_location="test_stage_negative_clone.txt",
        upload_size=-1,
        upload_checksum="",
        uploader="test",
        upload_name="test_stage_negative_clone.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 400

    decoded_response = CloneFailedResponse.model_validate_json(response.content)


def test_extreme_clone_size(
    client, server, orm
):
    """
    Tests that an upload size that is too large results in an error.
    """

    request = CloneInitiationRequest(
        destination_location="test_extreme_clone_size.txt",
        upload_size=1000000000000000000,
        upload_checksum="",
        uploader="test",
        upload_name="test_extreme_clone_size.txt",
        source="test_librarian",
        source_transfer_id=-1,
    )

    response = client.post("/api/v2/clone/stage", content=request.model_dump_json())

    assert response.status_code == 413

    # Check we can decode the response
    decoded_response = CloneFailedResponse.model_validate_json(response.content)