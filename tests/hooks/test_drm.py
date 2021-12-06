import requests

from ideafast_etl.hooks.drm import DreemHook


def test_metadata_call():
    """Test that the metadata call returns when payload contains dict items"""

    assert False


def test_metadata_call_empty():
    """Test that the metadata call returns an empty iterator when no dict items"""

    assert False


def test_download_file_success():
    """Test that a file is downloaded, and in the correct folder"""

    assert False


def test_download_file_no_data_url():
    """Test that no file is downloaded when data_url is ommitted"""

    assert False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    drm_hook = DreemHook()
    drm_hook.jwt_url = "http://test"
    drm_hook.user = "test_user"
    drm_hook.passw = "test_passw"

    result = drm_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)
