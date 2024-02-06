"""
Tests the ping API endpoint.
"""

from hera_librarian.models.ping import PingRequest, PingResponse


def test_ping(test_client):
    request = PingRequest()
    response = test_client.post(
        "/api/v2/ping",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
        auth=("test", "test"),
    )
    assert response.status_code == 200
    # Check we can decode the response
    response = PingResponse.model_validate_json(response.content)


def test_ping_logged_not_logged(test_client):
    request = PingRequest()
    response = test_client.post(
        "/api/v2/ping/logged",
        headers={"Content-Type": "application/json"},
        content=request.model_dump_json(),
        auth=("test", "test-not-real-password"),
    )
    assert response.status_code == 401
