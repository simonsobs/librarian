"""
Tests for the create archive task.
"""

import shutil
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from hera_librarian.deletion import DeletionPolicy
from hera_librarian.exceptions import LibrarianHTTPError
from hera_librarian.models.archive import ArchiveManifestResponse

# All files made here live under this prefix, and every task in this module is
# scoped to it with a match_query. The candidate query in get_files looks at the
# whole database, and the other modules in this package leave files behind.
PREFIX = "archive_test"


def prep_file(
    garbage_file, test_orm, session, file_name, age_days=0, size=None, available=True
):
    """
    Make a file with a single instance, optionally aged and resized.
    """

    store = session.query(test_orm.StoreMetadata).filter_by(ingestable=True).first()

    info = store.store_manager.path_info(garbage_file)
    store_path = store.store_manager.store(Path(file_name))

    shutil.copy(garbage_file, store_path)

    file = test_orm.File.new_file(
        filename=file_name,
        size=info.size if size is None else size,
        checksum=info.checksum,
        uploader="test_user",
        source="test_source",
    )

    instance = test_orm.Instance.new_instance(
        path=store_path, file=file, store=store, deletion_policy=DeletionPolicy.ALLOWED
    )

    file.create_time = file.create_time - timedelta(days=age_days)
    instance.created_time = file.create_time
    instance.available = available

    session.add_all([file, instance])
    session.commit()

    return file, instance


def add_archivist(test_orm, session, name="test_archivist"):
    """
    Register an archivist without pinging it.
    """

    archivist = test_orm.Archivist.new_archivist(
        name=name,
        url="http://localhost",
        port=8080,
        authenticator="archuser:secret",
        check_connection=False,
    )

    session.add(archivist)
    session.commit()

    return archivist


def fake_client(monkeypatch, test_orm, archive_id="archive-1", manifest_id=None):
    """
    Replace Archivist.client() with a mock that records the manifest it was
    given and echoes its manifest_id back, unless one is forced.

    Returns the mock, whose .post.call_args holds the request that was sent.
    """

    client = MagicMock()

    def post(endpoint, request, response):
        return ArchiveManifestResponse(
            archive_id=archive_id,
            manifest_id=request.manifest_id if manifest_id is None else manifest_id,
        )

    client.post = MagicMock(side_effect=post)

    monkeypatch.setattr(test_orm.Archivist, "client", lambda self: client)

    return client


def make_task(match_query=f"{PREFIX}%", **kwargs):
    from librarian_background.create_archive import CreateArchive

    settings = dict(
        name="Archive creator",
        soft_timeout="6:00:00",
        archivist_name="test_archivist",
        age_in_days=5,
        filesize_per_run=1024 * 1024,
        match_query=match_query,
    )
    settings.update(kwargs)

    return CreateArchive(**settings)


@pytest.fixture(scope="function")
def clean_archives(test_server, test_orm):
    """
    Remove every archive, claim, archivist and prefixed file this module makes.
    Candidate selection is global, so leaking any of these would change the
    result of a later test.
    """

    yield

    _, get_session, _ = test_server

    with get_session() as session:
        for link in session.query(test_orm.FileToArchives).all():
            session.delete(link)

        for archive in session.query(test_orm.Archive).all():
            session.delete(archive)

        for archivist in session.query(test_orm.Archivist).all():
            session.delete(archivist)

        session.commit()

        for file in (
            session.query(test_orm.File)
            .filter(test_orm.File.name.like(f"{PREFIX}%"))
            .all()
        ):
            file.delete(session=session, commit=False, force=True)

        session.commit()


def test_get_files_selects_aged_available_files(
    test_client, test_server, test_orm, garbage_file, clean_archives
):
    """
    A candidate is older than age_in_days, has an available instance, and
    matches match_query. A file with several available instances is offered
    exactly once, through its lowest-numbered instance.
    """

    _, get_session, _ = test_server
    session = get_session()

    old, old_instance = prep_file(
        garbage_file, test_orm, session, f"{PREFIX}/old.g3", age_days=10
    )
    prep_file(garbage_file, test_orm, session, f"{PREFIX}/young.g3", age_days=2)
    prep_file(garbage_file, test_orm, session, f"{PREFIX}/notes.txt", age_days=10)
    prep_file(
        garbage_file,
        test_orm,
        session,
        f"{PREFIX}/unavailable.g3",
        age_days=10,
        available=False,
    )

    # A second available instance of the old file, which must not duplicate it.
    store = session.query(test_orm.StoreMetadata).filter_by(ingestable=True).first()
    session.add(
        test_orm.Instance.new_instance(
            path=old_instance.path,
            file=old,
            store=store,
            deletion_policy=DeletionPolicy.ALLOWED,
        )
    )
    session.commit()

    files = make_task().get_files(session)

    assert sorted(file.name for file, _ in files) == [
        f"{PREFIX}/notes.txt",
        f"{PREFIX}/old.g3",
    ]

    # Narrowing the pattern drops the text file, and the instance we are handed
    # for the two-instance file is the first one.
    files = make_task(match_query=f"{PREFIX}%.g3").get_files(session)

    assert [file.name for file, _ in files] == [f"{PREFIX}/old.g3"]
    assert files[0][1].id == old_instance.id

    session.close()


def test_get_files_budget_and_existing_claims(
    test_client, test_server, test_orm, garbage_file, clean_archives
):
    """
    Files are accumulated oldest-first until filesize_per_run is exhausted, with
    the budget inclusive, and anything already claimed in files_to_archives is
    never offered again.
    """

    _, get_session, _ = test_server
    session = get_session()

    for index in range(5):
        prep_file(
            garbage_file,
            test_orm,
            session,
            f"{PREFIX}/budget_{index}.txt",
            age_days=10 + (5 - index),
            size=100,
        )

    # The oldest three come to exactly the budget, and are kept whole.
    files = make_task(filesize_per_run=300).get_files(session)

    assert [file.name for file, _ in files] == [
        f"{PREFIX}/budget_0.txt",
        f"{PREFIX}/budget_1.txt",
        f"{PREFIX}/budget_2.txt",
    ]

    # One byte short of three files only pays for two.
    assert len(make_task(filesize_per_run=299).get_files(session)) == 2

    # Claim the two oldest, even though their archive is not confirmed yet.
    archive = test_orm.Archive(
        manifest_id="manifest-existing", archive_id=None, archive_path=None
    )
    session.add(archive)

    for index in range(2):
        archive.files.append(
            test_orm.FileToArchives(file_name=f"{PREFIX}/budget_{index}.txt")
        )

    session.commit()

    files = make_task(filesize_per_run=300).get_files(session)

    assert [file.name for file, _ in files] == [
        f"{PREFIX}/budget_2.txt",
        f"{PREFIX}/budget_3.txt",
        f"{PREFIX}/budget_4.txt",
    ]

    session.close()


def test_create_archive_records_claim_and_archive_id(
    test_client, test_server, test_orm, garbage_file, clean_archives, monkeypatch
):
    """
    A successful run claims its files, records the archive ID the archivist
    minted, and leaves nothing behind for the next run to pick up. Without a
    matching archivist it does not reach out at all.
    """

    from librarian_server.settings import server_settings

    _, get_session, _ = test_server
    session = get_session()

    for index in range(3):
        prep_file(
            garbage_file, test_orm, session, f"{PREFIX}/happy_{index}.txt", age_days=10
        )

    client = fake_client(monkeypatch, test_orm, archive_id="archive-happy")

    # No archivist by that name: nothing is claimed and nothing is sent.
    make_task(archivist_name="does_not_exist").core(session=session)

    assert session.query(test_orm.Archive).count() == 0
    assert client.post.call_count == 0

    add_archivist(test_orm, session)

    task = make_task()
    task.core(session=session)

    archive = session.query(test_orm.Archive).one()

    assert archive.archive_id == "archive-happy"
    assert archive.archive_path is None
    assert sorted(link.file_name for link in archive.files) == [
        f"{PREFIX}/happy_0.txt",
        f"{PREFIX}/happy_1.txt",
        f"{PREFIX}/happy_2.txt",
    ]

    request = client.post.call_args.kwargs["request"]

    assert request.librarian_name == server_settings.name
    assert request.manifest_id == archive.manifest_id
    assert all(entry.outgoing_transfer_id == -1 for entry in request.archive_files)

    # The files are claimed, so a second run has nothing to do.
    task.core(session=session)

    assert session.query(test_orm.Archive).count() == 1
    assert client.post.call_count == 1

    session.close()


def test_create_archive_claim_is_released_only_on_failure(
    test_client, test_server, test_orm, garbage_file, clean_archives, monkeypatch
):
    """
    A request the archivist rejects releases the claim, so the files come back
    around. A request it accepts but answers with the wrong manifest ID does
    not: it has the files, so the archive is kept for manual reconciliation.
    """

    _, get_session, _ = test_server
    session = get_session()

    add_archivist(test_orm, session)

    for index in range(2):
        prep_file(
            garbage_file,
            test_orm,
            session,
            f"{PREFIX}/retried_{index}.txt",
            age_days=10,
        )

    client = fake_client(monkeypatch, test_orm)
    client.post = MagicMock(
        side_effect=LibrarianHTTPError(
            url="archive",
            status_code=500,
            reason="No thank you",
            suggested_remedy="Try later",
        )
    )

    make_task().core(session=session)

    assert session.query(test_orm.Archive).count() == 0
    assert session.query(test_orm.FileToArchives).count() == 0
    assert len(make_task().get_files(session)) == 2

    # Same files, but this time the archivist takes them and disagrees about
    # which request it was answering.
    fake_client(
        monkeypatch,
        test_orm,
        archive_id="archive-confused",
        manifest_id="not-the-one-we-sent",
    )

    make_task().core(session=session)

    archive = session.query(test_orm.Archive).one()

    assert archive.archive_id == "archive-confused"
    assert archive.manifest_id != "not-the-one-we-sent"
    assert len(archive.files) == 2
    assert make_task().get_files(session) == []

    session.close()
