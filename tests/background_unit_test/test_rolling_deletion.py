"""
Tests for the rolling deletion task.
"""

import shutil
from pathlib import Path

from hera_librarian.deletion import DeletionPolicy


def test_rolling_deletion_with_single_instance(
    test_client, test_server, test_orm, garbage_file
):
    """
    Delete a single instance.
    """
    from librarian_background.rolling_deletion import RollingDeletion

    _, get_session, _ = test_server

    session = get_session()

    store = session.query(test_orm.StoreMetadata).filter_by(ingestable=True).first()

    info = store.store_manager.path_info(garbage_file)

    FILE_NAME = "path/for/rolling/deletion"

    store_path = store.store_manager.store(Path(FILE_NAME))

    shutil.copy(garbage_file, store_path)

    # Create file and instances
    file = test_orm.File.new_file(
        filename=FILE_NAME,
        size=info.size,
        checksum=info.checksum,
        uploader="test_user",
        source="test_source",
    )

    instance = test_orm.Instance.new_instance(
        path=store_path, file=file, store=store, deletion_policy=DeletionPolicy.ALLOWED
    )

    session.add_all([file, instance])
    session.commit()

    INSTANCE_ID = instance.id

    # Run the task
    task = RollingDeletion(
        name="Rolling deletion",
        soft_timeout="6:00:00",
        store_name=store.name,
        age_in_days=0.0000000000000000001,
        number_of_remote_copies=0,
        verify_downstream_checksums=False,
        mark_unavailable=False,
        force_deletion=False,
    )()

    assert task

    # Check that the instance is gone
    assert (
        session.query(test_orm.Instance).filter_by(id=INSTANCE_ID).one_or_none() is None
    )

    # Delete the file we created
    session.get(test_orm.File, FILE_NAME).delete(
        session=session, commit=True, force=True
    )

    return
