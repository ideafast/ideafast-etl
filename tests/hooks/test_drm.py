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

    mock_conn = Mock()
    mock_conn.get.return_value.json.return_value = {"data_url": "test_url"}
    mock_requests_drm.get.return_value.__enter__.return_value.headers.get.return_value = (
        2048
    )
    mock_requests_drm.get.return_value.__enter__.return_value.iter_content.return_value = iter(
        [
            "chunk1".encode("utf-8"),
            "_".encode("utf-8"),
            "chunk2".encode("utf-8"),
        ]
    )

    with patch.object(DreemHook, "get_conn", return_value=mock_conn):
        drm_hook = DreemHook()
        drm_hook.base_url = "test"

        result = drm_hook.download_file("test_ref", tmp_path)

        with open(tmp_path / "test_ref.h5") as f:
            stored = f.read()
            assert stored == "chunk1_chunk2"
            assert len(list(tmp_path.iterdir())) == 1


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
