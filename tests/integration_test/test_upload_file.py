"""
Tests that you can successfully upload files to the server.
"""

import sqlite3
from pathlib import Path


def test_upload_simple(librarian_client, garbage_file, server):
    """
    Also tests file searching.
    """
    # Perform the upload
    librarian_client.upload(garbage_file, Path("test_file"))

    # Check we got it!
    # TODO: Implement that check within the librarian client (i.e. the ability to search db)

    # Check we got it (by manually verifying)
    conn = sqlite3.connect(server.database)
    c = conn.cursor()
    res = c.execute("SELECT path FROM instances WHERE file_name='test_file'")
    real_file_path = Path(res.fetchone()[0])

    assert real_file_path.exists()

    with open(real_file_path, "rb") as handle:
        real_file_contents = handle.read()

    with open(garbage_file, "rb") as handle:
        garbage_file_contents = handle.read()

    assert real_file_contents == garbage_file_contents

    search_result = librarian_client.search_files(name="test_file")

    assert len(search_result) == 1

    assert search_result[0].name == "test_file"
    assert search_result[0].size == 1024

    assert search_result[0].instances[0].path == real_file_path


def test_upload_file_to_unique_directory(librarian_client, garbage_file, server):
    librarian_client.upload(garbage_file, Path("test_directory/test_file"))

    # Check we got it (by manually verifying)
    conn = sqlite3.connect(server.database)
    c = conn.cursor()
    res = c.execute(
        "SELECT path FROM instances WHERE file_name='test_directory/test_file'"
    )
    real_file_path = Path(res.fetchone()[0])

    assert real_file_path.exists()

    with open(real_file_path, "rb") as handle:
        real_file_contents = handle.read()

    with open(garbage_file, "rb") as handle:
        garbage_file_contents = handle.read()

    assert real_file_contents == garbage_file_contents
