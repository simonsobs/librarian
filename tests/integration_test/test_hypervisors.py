"""
Tests for the transfer hypervisors.
"""

from pathlib import Path

from hera_librarian.transfer import TransferStatus


def test_handle_stale_incoming_transfer(
    test_server_with_many_files_and_errors,
    test_orm,
    mocked_admin_client,
    server,
    admin_client,
    librarian_database_session_maker,
    garbage_file,
):
    from librarian_background.hypervisor import handle_stale_incoming_transfer

    assert mocked_admin_client.add_librarian(
        name="live_server",
        url="http://localhost",
        authenticator="admin:password",  # This is the default authenticator.
        port=server.id,
    )

    admin_client.upload(garbage_file, Path("name/of/file/to/use/for/transfers"))

    assert admin_client.add_librarian(
        name="test_server",
        url="http://localhost",
        authenticator="admin:password",  # This is the default authenticator.
        port=test_server_with_many_files_and_errors[2].id,
        check_connection=False,
    )

    source_session_maker = test_server_with_many_files_and_errors[1]

    def make_source_and_destination(
        source_status: TransferStatus, destination_status: TransferStatus
    ) -> tuple[int]:
        with librarian_database_session_maker() as session:
            file = session.get(test_orm.File, "name/of/file/to/use/for/transfers")
            instance = file.instances[0]

            source = test_orm.OutgoingTransfer.new_transfer(
                destination="test_server",
                instance=instance,
                file=file,
            )

            source.status = source_status

            session.add(source)
            session.commit()

            source_id = source.id

        with source_session_maker() as session:
            destination = test_orm.IncomingTransfer.new_transfer(
                uploader="live_server",
                upload_name="name/of/file/to/use/for/transfers",
                source="live_server",
                transfer_size=1024,
                transfer_checksum="Never_Mind",
            )

            destination.status = destination_status
            destination.source_transfer_id = source_id

            session.add(destination)
            session.commit()

            destination_id = destination.id

        return source_id, destination_id

    def delete_transfers(source_transfer: int, destination_transfer: int):
        with librarian_database_session_maker() as session:
            transfer = session.get(test_orm.OutgoingTransfer, source_transfer)
            session.delete(transfer)
            session.commit()

        with source_session_maker() as session:
            transfer = session.get(test_orm.IncomingTransfer, destination_transfer)
            session.delete(transfer)
            session.commit()

    # Let's cover all the cases.
    # a) Remote transfer is cancelled.
    # b) Remote transfer has the same status as us
    # c) Remote transfer is STAGED when we are INITIATED
    # d) Remote transfer is INITIATED when are we ONGOING
    # e) Remote transfer is STAGED when we are ONGOING

    # --- a ---
    source, destination = make_source_and_destination(
        TransferStatus.CANCELLED, TransferStatus.INITIATED
    )

    with source_session_maker() as session:
        transfer = session.get(test_orm.IncomingTransfer, destination)
        handle_stale_incoming_transfer(session, transfer=transfer)
        assert (
            session.get(test_orm.IncomingTransfer, destination).status
            == TransferStatus.FAILED
        )

    delete_transfers(source, destination)

    # --- b ---
    source, destination = make_source_and_destination(
        TransferStatus.INITIATED, TransferStatus.INITIATED
    )

    with source_session_maker() as session:
        transfer = session.get(test_orm.IncomingTransfer, destination)
        assert handle_stale_incoming_transfer(session, transfer=transfer)
        assert (
            session.get(test_orm.IncomingTransfer, destination).status
            == TransferStatus.INITIATED
        )

    delete_transfers(source, destination)

    # --- c ---
    source, destination = make_source_and_destination(
        TransferStatus.STAGED, TransferStatus.INITIATED
    )

    with source_session_maker() as session:
        transfer = session.get(test_orm.IncomingTransfer, destination)
        handle_stale_incoming_transfer(session, transfer=transfer)
        assert (
            session.get(test_orm.IncomingTransfer, destination).status
            == TransferStatus.STAGED
        )

    delete_transfers(source, destination)

    # --- d ---
    source, destination = make_source_and_destination(
        TransferStatus.INITIATED, TransferStatus.ONGOING
    )

    with source_session_maker() as session:
        transfer = session.get(test_orm.IncomingTransfer, destination)
        handle_stale_incoming_transfer(session, transfer=transfer)
        assert (
            session.get(test_orm.IncomingTransfer, destination).status
            == TransferStatus.FAILED
        )

    delete_transfers(source, destination)

    # --- e ---
    source, destination = make_source_and_destination(
        TransferStatus.STAGED, TransferStatus.ONGOING
    )

    with source_session_maker() as session:
        transfer = session.get(test_orm.IncomingTransfer, destination)
        handle_stale_incoming_transfer(session, transfer=transfer)
        assert (
            session.get(test_orm.IncomingTransfer, destination).status
            == TransferStatus.STAGED
        )

    delete_transfers(source, destination)

    # Remove the librarians we added.
    with librarian_database_session_maker() as session:
        file = session.get(test_orm.File, "name/of/file/to/use/for/transfers")
        file.delete(session=session, commit=True, force=True)

    assert mocked_admin_client.remove_librarian(name="live_server")
    assert admin_client.remove_librarian(name="test_server")