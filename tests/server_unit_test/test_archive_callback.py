"""
Tests for the archive callback endpoint, which the archivist calls to report
where it has stored an archive.
"""

import pytest

from hera_librarian.models.archive import (
    ArchiveCallbackFailResponse,
    ArchiveCallbackRequest,
    ArchiveCallbackResponse,
)

# The password deliberately contains a colon: the authenticator is stored as
# "user:password" and is split with partition(), so this would break under split().
ARCHIVIST_NAME = "test_archivist"
ARCHIVIST_AUTH = ("archuser", "se:cret")
ARCHIVE_ID = "archive-callback-test"


@pytest.fixture(scope="function")
def archivist_and_archive(test_server, test_orm):
    """
    A registered archivist and an archive of theirs that has been submitted but
    not yet confirmed.
    """

    _, get_session, _ = test_server

    with get_session() as session:
        archivist = test_orm.Archivist.new_archivist(
            name=ARCHIVIST_NAME,
            url="http://localhost",
            port=8080,
            authenticator=":".join(ARCHIVIST_AUTH),
            check_connection=False,
        )

        archive = test_orm.Archive(
            manifest_id="manifest-callback-test",
            archive_id=ARCHIVE_ID,
            archive_path=None,
        )

        session.add_all([archivist, archive])
        session.commit()

    yield

    with get_session() as session:
        session.delete(
            session.query(test_orm.Archivist).filter_by(name=ARCHIVIST_NAME).one()
        )
        session.delete(
            session.query(test_orm.Archive).filter_by(archive_id=ARCHIVE_ID).one()
        )
        session.commit()


def post_callback(test_client, auth=ARCHIVIST_AUTH, **kwargs):
    settings = dict(
        archivist_name=ARCHIVIST_NAME,
        manifest_id="manifest-callback-test",
        archive_id=ARCHIVE_ID,
        archive_path="/store/archive.tar",
    )
    settings.update(kwargs)

    return test_client.post_with_auth(
        "/api/v2/archive/callback",
        content=ArchiveCallbackRequest(**settings).model_dump_json(),
        auth=auth,
    )


def test_callback_records_and_guards_archive_path(
    test_client, test_server, test_orm, archivist_and_archive
):
    """
    The reported path is written to the archive. A retry of the same callback is
    harmless, but one reporting a different path is refused and leaves the
    original in place, and an archive we have no record of is a 404.
    """

    response = post_callback(test_client)

    assert response.status_code == 200
    assert ArchiveCallbackResponse.model_validate_json(response.content).success

    with test_server[1]() as session:
        archive = session.query(test_orm.Archive).filter_by(archive_id=ARCHIVE_ID).one()

        assert archive.archive_path == "/store/archive.tar"

    # The archivist retrying with the same information must not be an error.
    assert post_callback(test_client).status_code == 200

    response = post_callback(test_client, archive_path="/store/somewhere_else.tar")

    assert response.status_code == 406

    response = post_callback(test_client, archive_id="never-heard-of")

    assert response.status_code == 404
    assert (
        "never-heard-of"
        in ArchiveCallbackFailResponse.model_validate_json(response.content).reason
    )

    with test_server[1]() as session:
        archive = session.query(test_orm.Archive).filter_by(archive_id=ARCHIVE_ID).one()

        assert archive.archive_path == "/store/archive.tar"


def test_callback_rejects_anyone_but_the_archivist(
    test_client, test_server, test_orm, archivist_and_archive
):
    """
    Only the exact credential pair we hold for the named archivist is accepted.
    A perfectly valid librarian account is still not the archivist: that is the
    regression authenticating against the stored authenticator, rather than
    against the users table, exists to close.
    """

    response = post_callback(test_client, auth=("admin", "password"))

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Basic"
    assert not ArchiveCallbackFailResponse.model_validate_json(response.content).success

    assert (
        post_callback(test_client, auth=(ARCHIVIST_AUTH[0], "wrong")).status_code == 401
    )
    assert (
        post_callback(test_client, archivist_name="not_an_archivist").status_code == 401
    )

    with test_server[1]() as session:
        archive = session.query(test_orm.Archive).filter_by(archive_id=ARCHIVE_ID).one()

        assert archive.archive_path is None

        # The check underneath the endpoint, including that a password
        # containing a colon survives the round trip through the authenticator.
        def check(
            name=ARCHIVIST_NAME, user=ARCHIVIST_AUTH[0], secret=ARCHIVIST_AUTH[1]
        ):
            return test_orm.Archivist.check_archivist(
                name=name, username=user, password=secret, session=session
            )

        assert check().name == ARCHIVIST_NAME
        assert check(user="someone_else") is None
        assert check(secret="se") is None
        assert check(name="not_an_archivist") is None
