import requests

from ideafast_etl.hooks.wks import WildkeysHook

# TODO: add tests once API is set up and approach is agreed


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    wks_hook = WildkeysHook()
    wks_hook.jwt_url = "http://test"
    wks_hook.user = "test_user"
    wks_hook.passw = "test_passw"

    result = wks_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)
