"""
Tests for the corruption endpoints. We need to use the integration test here because these
endpoints require a server that is contactable and that we can use async transfer managers for.
"""

import random
from pathlib import Path

from hera_librarian.authlevel import AuthLevel


def test_fix_missing_file(
    test_server_with_many_files_and_errors,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    tmp_path,
):
    # Create the accounts for the opposing servers
    mocked_admin_client.create_user(
        username="live_server",
        password="password",
        auth_level=AuthLevel.CALLBACK,
    )

    admin_client.create_user(
        username="test_server",
        password="password",
        auth_level=AuthLevel.READAPPEND,
    )

    # Reigster the librarians with each other
    assert mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="test_server:password",
        port=server.id,
    )

    assert admin_client.add_librarian(
        name="test_server",
        url="http://localhost",
        authenticator="live_server:password",  # This is the default authenticator.
        port=test_server_with_many_files_and_errors[2].id,
        check_connection=False,
    )

    # Need to add a bunch of files to the source server

    file_cores = [f"repair_test_item_{x}.txt" for x in range(2)]
    file_names = [f"repair_test/{file}" for file in file_cores]

    for file in file_cores:
        with open(tmp_path / file, "w") as handle:
            handle.write(str(random.randbytes(1024)))

        mocked_admin_client.upload(tmp_path / file, Path(f"repair_test/{file}"))

    # Mock up a couple of remote instances for these files on the destination, we must have these
    # to use the repair workflow!
    with test_server_with_many_files_and_errors[1]() as session:
        for file_name in file_names:
            file = (
                session.query(test_orm.File)
                .filter(test_orm.File.name == file_name)
                .one()
            )
            librarian = (
                session.query(test_orm.Librarian)
                .filter(test_orm.Librarian.name == "live_server")
                .one()
            )
            instance = test_orm.RemoteInstance.new_instance(
                file=file,
                store_id=1,
                librarian=librarian,
            )
            session.add(instance)
        session.commit()

    # Now use the repair tasks to ask for copies of those files!
    from hera_librarian.models.corrupt import (
        CorruptionPreparationRequest,
        CorruptionPreparationResponse,
        CorruptionResendRequest,
        CorruptionResendResponse,
    )

    from ..conftest import create_test_client, make_mocked_admin_client

    # Ok, so we need to re-roll this process.
    repair_request_client = make_mocked_admin_client(
        create_test_client(
            test_server_with_many_files_and_errors,
            username="live_server",
            password="password",
        ),
        username="live_server",
        password="password",
    )

    for file_name in file_names:
        assert repair_request_client.post(
            "corrupt/prepare",
            request=CorruptionPreparationRequest(
                file_name=file_name, librarian_name="live_server"
            ),
            response=CorruptionPreparationResponse,
        ).ready

    transfer_ids = []

    for file_name in file_names:
        resp = repair_request_client.post(
            "corrupt/resend",
            request=CorruptionResendRequest(
                file_name=file_name, librarian_name="live_server"
            ),
            response=CorruptionResendResponse,
        )

        assert resp.success
        transfer_ids.append(resp.destination_transfer_id)

    # Check in on those transfer IDs to see if they've been created on the downstream
    with librarian_database_session_maker() as session:
        for transfer_id in transfer_ids:
            assert (
                session.query(test_orm.IncomingTransfer).filter_by(id=transfer_id).one()
            )

    # Now run the send queue and checkin tasks to get the files to the destination
    from librarian_background.queues import CheckConsumedQueue, ConsumeQueue
    from librarian_background.recieve_clone import RecieveClone

    consume_task = ConsumeQueue(name="consume_queue")
    consume_task.core(session_maker=test_server_with_many_files_and_errors[1])

    checkin_task = CheckConsumedQueue(name="checkin_queue")
    checkin_task.core(session_maker=test_server_with_many_files_and_errors[1])

    # Now we should have the files on the destination server and they need to be ingested
    with librarian_database_session_maker() as session:
        recv_task = RecieveClone(name="recieve_clone_job")
        recv_task.core(session=session)

        for file_name in file_names:
            assert session.query(test_orm.File).filter_by(name=file_name).one()

    # Delete it all.
    with librarian_database_session_maker() as session:
        for file_name in file_names:
            file = session.query(test_orm.File).filter_by(name=file_name).one()
            file.delete(session=session, commit=False, force=True)

            # Incoming and outgoing transfers
            inc_transfers = (
                session.query(test_orm.IncomingTransfer)
                .filter_by(upload_name=file_name)
                .all()
            )
            out_transfers = (
                session.query(test_orm.OutgoingTransfer)
                .filter_by(file_name=file_name)
                .all()
            )

            for transfer in inc_transfers:
                session.delete(transfer)

            for transfer in out_transfers:
                if transfer.send_queue:
                    session.delete(transfer.send_queue)
                session.delete(transfer)

        session.commit()

    with test_server_with_many_files_and_errors[1]() as session:
        for file_name in file_names:
            file = session.query(test_orm.File).filter_by(name=file_name).one()
            file.delete(session=session, commit=False, force=True)

            # Incoming and outgoing transfers
            inc_transfers = (
                session.query(test_orm.IncomingTransfer)
                .filter_by(upload_name=file_name)
                .all()
            )
            out_transfers = (
                session.query(test_orm.OutgoingTransfer)
                .filter_by(file_name=file_name)
                .all()
            )

            for transfer in inc_transfers:
                session.delete(transfer)

            for transfer in out_transfers:
                if transfer.send_queue:
                    session.delete(transfer.send_queue)
                session.delete(transfer)

        session.commit()

    # Now delete accounts and librarians
    mocked_admin_client.delete_user("live_server")
    admin_client.delete_user("test_server")

    mocked_admin_client.remove_librarian("live_server")
    admin_client.remove_librarian("test_server")
