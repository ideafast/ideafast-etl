import json
from json.decoder import JSONDecodeError
from unittest.mock import MagicMock

import requests
from requests.cookies import MockRequest

from ideafast_etl.hooks.dmp import DmpHook


def test_upload_success(temp_file, mock_get_connection, mock_requests_general):

    tmp_file, dmp_dataset = temp_file

    block_size, blocks = (8192, 100)
    with open(tmp_file, "wb") as output_file:
        [output_file.write(bytearray(block_size)) for _ in range(blocks)]

    def read_monitor(*args, **kwargs):
        # simulate uploading by updating the monitor
        data = kwargs.get("data")
        [data.read(block_size) for _ in range(blocks + 1)]
        return MagicMock()

    mock_requests_general.Session().post.return_value.json.return_value = {}
    mock_requests_general.Session().post.side_effect = read_monitor

    assert tmp_file.stat().st_size == block_size * blocks
    print(tmp_file.stat().st_size)

    dmp_hook = DmpHook()

    result = dmp_hook.upload(dmp_dataset, tmp_file)

    assert result is True


def test_upload_error(temp_file, mock_get_connection, mock_requests_general):
    mock_requests_general.Session().post.return_value.json.return_value = {
        "errors": "error"
    }
    tmp_file, dmp_dataset = temp_file
    tmp_file.write_text("placeholder")
    dmp_hook = DmpHook()

    # raises an exception, however - is passed with logging
    result = dmp_hook.upload(dmp_dataset, tmp_file)

    assert result is False


def test_upload_error_no_json(temp_file, mock_get_connection, mock_requests_general):
    # force a decode error
    mock_requests_general.Session().post.return_value.json.side_effect = (
        json.decoder.JSONDecodeError("", "", 0)
    )
    tmp_file, dmp_dataset = temp_file
    dmp_hook = DmpHook()

    # raises an exception, however - is passed with logging
    result = dmp_hook.upload(dmp_dataset, tmp_file)

    assert result is False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    dmp_hook = DmpHook()
    dmp_hook.jwt_url = "http://test"
    dmp_hook.user = "test_user"
    dmp_hook.passw = "test_passw"

    result = dmp_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)


def test_rm_local_data(temp_file):
    """Test that local data is all removed, both zip and unzipped folder"""
    tmp_file, dmp_dataset = temp_file
    zip_path = DmpHook.zip_folder(tmp_file.parent)

    result = DmpHook.rm_local_data(zip_path)

    assert not zip_path.exists()
    assert len(list(zip_path.parent.iterdir())) == 0


def test_rm_local_data_not_zipped_yet(temp_file):
    """Test that local data is all removed, even when not zipped yet"""
    tmp_file, dmp_dataset = temp_file
    tmp_folder = tmp_file.parent
    non_existing_zip_path = tmp_folder.parent / (tmp_folder.name + ".zip")

    result = DmpHook.rm_local_data(non_existing_zip_path)

    assert not non_existing_zip_path.exists()
    assert len(list(tmp_folder.parent.iterdir())) == 0


def test_rm_local_data_only_zipped(temp_file):
    """Test that local data is all removed, even when only zip folder exists"""
    tmp_file, dmp_dataset = temp_file
    zip_path = DmpHook.zip_folder(tmp_file.parent)
    tmp_file.unlink()
    tmp_file.parent.rmdir()

    result = DmpHook.rm_local_data(zip_path)

    assert not zip_path.exists()
    assert len(list(zip_path.parent.iterdir())) == 0


def test_zip_folder(temp_file):
    """Test that a folder is successfully zipped"""

    tmp_file, dmp_dataset = temp_file

    result = DmpHook.zip_folder(tmp_file.parent)

    assert result.exists()
    assert len(list(result.parent.iterdir())) == 2
