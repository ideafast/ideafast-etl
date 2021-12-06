import requests

from ideafast_etl.hooks.dmp import DmpHook


def test_upload_success():

    assert False


def test_upload_error():

    assert False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    dmp_hook = DmpHook()
    dmp_hook.jwt_url = "http://test"
    dmp_hook.user = "test_user"
    dmp_hook.passw = "test_passw"

    result = dmp_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)
