import pytest
import requests

from ideafast_etl.hooks.dmp import DmpHook
from tests.hooks.conftest import mock_requests


def test_upload_success(tmp_path, mock_get_connection, mock_requests_general):
    mock_requests_general.Session().post.return_value.json.return_value = {}
    dmp_dataset = "NR1DEVICE-APATIENT-20211213-20211213"
    tmp_file = tmp_path / f"{dmp_dataset}.txt"
    tmp_file.write_text("placeholder")
    dmp_hook = DmpHook()

    result = dmp_hook.upload(dmp_dataset, tmp_file)

    assert result is True


def test_upload_error(tmp_path, mock_get_connection, mock_requests_general):
    mock_requests_general.Session().post.return_value.json.return_value = {
        "errors": "error"
    }
    dmp_dataset = "NR1DEVICE-APATIENT-20211213-20211213"
    tmp_file = tmp_path / f"{dmp_dataset}.txt"
    tmp_file.write_text("placeholder")
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
