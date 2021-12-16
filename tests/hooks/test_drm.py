from pathlib import Path
from unittest.mock import Mock, patch

import requests

from ideafast_etl.hooks.drm import DreemHook


def test_metadata_call():
    """Test that the metadata call returns when payload contains dict items"""
    mock_conn = Mock()
    mock_conn.get.return_value.json.return_value = {
        "results": [{"nr": x} for x in range(5)]
    }

    with patch.object(DreemHook, "get_conn", return_value=mock_conn):
        drm_hook = DreemHook()
        drm_hook.base_url = "test"
        drm_hook.extras = {"user_id": "test_id"}

        result = [r for r in drm_hook.get_metadata()]

        assert len(result) == 5
        assert result[4].get("nr") == 4


def test_metadata_call_empty():
    """Test that the metadata call returns an empty iterator when no dict items"""
    mock_conn = Mock()
    mock_conn.get.return_value.json.return_value = {"results": []}

    with patch.object(DreemHook, "get_conn", return_value=mock_conn):
        drm_hook = DreemHook()
        drm_hook.base_url = "test"
        drm_hook.extras = {"user_id": "test_id"}

        result = [r for r in drm_hook.get_metadata()]

        assert len(result) == 0


def test_download_file_success(mock_requests_drm, tmp_path):
    """Test that a file is downloaded, and in the correct folder"""
    chunk_size = 1024
    total_chunks = 3
    mock_conn = Mock()
    mock_conn.get.return_value.json.return_value = {"data_url": "test_url"}
    mock_requests_drm.get.return_value.__enter__.return_value.headers.get.return_value = (
        chunk_size * total_chunks
    )
    mock_requests_drm.get.return_value.__enter__.return_value.iter_content.return_value = iter(
        # adding None to the list to invoke the if-statements to resolve to False
        [bytearray(chunk_size) for _ in range(total_chunks)]
        + [None]
    )

    with patch.object(DreemHook, "get_conn", return_value=mock_conn):
        drm_hook = DreemHook()
        drm_hook.base_url = "test"

        result = drm_hook.download_file("test_ref", tmp_path)

        assert len(list(tmp_path.iterdir())) == 1
        assert (tmp_path / "test_ref.h5").stat().st_size == chunk_size * total_chunks


def test_download_file_no_data_url(tmp_path):
    """Test that no file is downloaded when data_url is ommitted"""
    mock_conn = Mock()
    mock_conn.get.return_value.json.return_value = {"data_url": ""}

    with patch.object(DreemHook, "get_conn", return_value=mock_conn):
        drm_hook = DreemHook()
        drm_hook.base_url = "test"

        result = drm_hook.download_file("test_ref", tmp_path)

        assert result is False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    drm_hook = DreemHook()
    drm_hook.jwt_url = "http://test"
    drm_hook.user = "test_user"
    drm_hook.passw = "test_passw"

    result = drm_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)
