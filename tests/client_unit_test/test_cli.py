# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD License

"""Test code in hera_librarian/cli.py

"""
from unittest.mock import MagicMock, patch

import pytest

from hera_librarian import cli
from hera_librarian.exceptions import LibrarianError


def test_die(capsys):
    # test without specifying replacement args
    with pytest.raises(SystemExit) as e:
        cli.die("my error")
    captured = capsys.readouterr()
    assert e.type == SystemExit
    assert e.value.code == 1
    assert captured.err == "error: my error\n"

    # test with replacement args
    with pytest.raises(SystemExit) as e:
        cli.die("my %s", "error")
    captured = capsys.readouterr()
    assert e.type == SystemExit
    assert e.value.code == 1
    assert captured.err == "error: my error\n"

    return


def test_print_table(capsys):
    # define dicts
    dict1 = {"name": "foo", "size": 10}
    dict2 = {"name": "bar", "size": 12}
    dict_list = [dict1, dict2]
    col_list = ["name", "size"]
    col_names = ["Name of file", "Size of file"]

    # test without specifying order
    cli.print_table(dict_list)
    captured = capsys.readouterr()
    stdout = captured.out
    correct_table = """name | size
---- | ----
foo  | 10  
bar  | 12  
"""
    assert stdout == correct_table

    # test without column names
    cli.print_table(dict_list, col_list)
    captured = capsys.readouterr()
    stdout = captured.out
    assert stdout == correct_table

    # test with column names
    cli.print_table(dict_list, col_list, col_names)
    captured = capsys.readouterr()
    stdout = captured.out
    correct_table = """Name of file | Size of file
------------ | ------------
foo          | 10          
bar          | 12          
"""
    assert stdout == correct_table

    # test using the wrong number of column headers
    with pytest.raises(ValueError):
        cli.print_table(dict_list, col_list, col_names[:1])

    return


def test_verify_file_success(capsys):
    # Mock the client and its verify_file_row method
    client_mock = MagicMock()
    client_mock.verify_file_row.return_value = {"verified": True}

    # Mock the get_client function to return the mock client
    with patch("hera_librarian.cli.get_client", return_value=client_mock):
        # Create a mock args object
        args = MagicMock()
        args.conn_name = "test_conn"
        args.name = "test_name"

        # Call the verify_file function
        cli.verify_file(args)

        # Assert that the verify_file_row method was called with the correct arguments
        client_mock.verify_file_row.assert_called_once_with(name=args.name)

        # Check the printed output
        captured = capsys.readouterr()
        assert "File verification successful." in captured.out


def test_verify_file_failure(capsys):
    client_mock = MagicMock()
    client_mock.verify_file_row.return_value = {"verified": False}

    # Mock the get_client function to return the mock client
    with patch("hera_librarian.cli.get_client", return_value=client_mock):
        # Create a mock args object
        args = MagicMock()
        args.conn_name = "test_conn"
        args.name = "test_name"

        # Call the verify_file function
        cli.verify_file(args)

        # Check the printed output
        captured = capsys.readouterr()
        assert "File verification failed." in captured.out


def test_verify_file_error():
    # Mock the client and its verify_file_row method to raise a LibrarianError
    client_mock = MagicMock()
    client_mock.verify_file_row.side_effect = LibrarianError("Test error")

    # Mock the get_client function to return the mock client
    with patch("hera_librarian.cli.get_client", return_value=client_mock):
        # Create a mock args object
        args = MagicMock()
        args.conn_name = "test_conn"
        args.name = "test_name"

        # Mock the die function to raise a SystemExit instead of exiting the program
        with patch("hera_librarian.cli.die", side_effect=SystemExit):
            # Call the verify_file function and assert that it raises a SystemExit
            with pytest.raises(SystemExit):
                cli.verify_file(args)


def test_sizeof_fmt():
    # test a few known values
    bts = 512
    assert cli.sizeof_fmt(bts) == "512.0 B"

    bts = 1024
    assert cli.sizeof_fmt(bts) == "1.0 kB"

    bts = 1024**2
    assert cli.sizeof_fmt(bts) == "1.0 MB"

    bts = 1024**3
    assert cli.sizeof_fmt(bts) == "1.0 GB"

    bts = 1024**4
    assert cli.sizeof_fmt(bts) == "1.0 TB"

    bts = 1024**5
    assert cli.sizeof_fmt(bts) == "1.0 PB"

    bts = 1024**6
    assert cli.sizeof_fmt(bts) == "1.0 EB"

    bts = 1024**7
    assert cli.sizeof_fmt(bts) == "1.0 ZB"

    bts = 1024**8
    assert cli.sizeof_fmt(bts) == "1.0 YB"

    return


def test_generate_parser():
    ap = cli.generate_parser()

    # make sure we have all the subparsers we're expecting
    available_subparsers = tuple(ap._subparsers._group_actions[0].choices.keys())
    assert "add-file-event" in available_subparsers
    assert "add-obs" in available_subparsers
    assert "launch-copy" in available_subparsers
    assert "assign-sessions" in available_subparsers
    assert "delete-files" in available_subparsers
    assert "locate-file" in available_subparsers
    assert "initiate-offload" in available_subparsers
    assert "offload-helper" in available_subparsers
    assert "search-files" in available_subparsers
    assert "set-file-deletion-policy" in available_subparsers
    assert "stage-files" in available_subparsers
    assert "upload" in available_subparsers
    assert "verify-file" in available_subparsers

    return
