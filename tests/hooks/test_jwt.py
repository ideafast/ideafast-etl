from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import jwt
import pytest
import requests

from ideafast_etl.hooks.jwt import JwtHook


@pytest.fixture(scope="module")
def haystack():
    """Return a haystack and needle for pathfinding"""
    needle = "needle"
    payload = {
        "haystack1": {"haystack2": {"haystack3": None, "haystack4": needle}},
        "haystack5": [{"haystack6": None}, {"haystack7": needle}],
        "haystack8": {},
    }
    return (payload, needle)


def test_find_jwt_token(haystack):
    """Test that a deep token can be found"""
    path = "haystack1.haystack2.haystack4"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, haystack[0])

    assert result == haystack[1]


def test_find_jwt_token_empty(haystack):
    """Test that a deep _empty_ token can be found"""
    path = "haystack8"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, haystack[0])

    assert result == "{}"


def test_find_jwt_token_with_list(haystack):
    """Test that a deep token can be found"""
    path = "haystack5.[1].haystack7"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, haystack[0])

    assert result == haystack[1]


def test_find_jwt_token_starts_with_list(haystack):
    """Test that a deep token can be found"""
    path = "[0].haystack5.[1].haystack7"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, [haystack[0], None])

    assert result == haystack[1]


@pytest.mark.xfail(raises=KeyError, strict=True)
def test_find_jwt_token_not_found(haystack):
    """Test that a deep token can _not_ be found"""
    path = "haystack5.[0].haystack70"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, haystack[0])

    assert result == haystack[1]


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    jwt_hook = JwtHook()
    jwt_hook.jwt_url = "http://test"
    jwt_hook.user = "test_user"
    jwt_hook.passw = "test_passw"

    result = jwt_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)


def test_jwt_get_token_still_valid(mock_requests, mock_airflow_settings):
    """
    Test that a jwt_token is returned if still valid

    i.e., not making a HTTP request or storing a new value in Airflow
    """
    jwt_token = jwt.encode({"some": "payload"}, "secret")
    jwt_hook = JwtHook()
    jwt_hook.jwt_token = jwt_token

    result = jwt_hook._get_jwt_token()

    assert result == jwt_token
    assert not mock_requests.called
    assert not mock_airflow_settings.called


def test_jwt_get_token_not_valid(mock_requests, mock_airflow_settings):
    """Test that a jwt_token is refreshed if no longer valid, and stored into connections"""
    mock_response = Mock()
    mock_response.json.return_value = {"jwt_key": "test_key"}
    mock_requests.Session().send.return_value = mock_response

    mock_airflow_settings.Session().query.return_value.filter.return_value.one.return_value.extra = (
        "{}"
    )

    jwt_token = jwt.encode(
        {"exp": datetime.now(tz=timezone.utc) - timedelta(seconds=30)}, "secret"
    )
    jwt_hook = JwtHook()
    jwt_hook.jwt_token = jwt_token
    jwt_hook.jwt_token_path = "jwt_key"

    result = jwt_hook._get_jwt_token()

    assert result == "test_key"
    mock_airflow_settings.Session().commit.assert_called_once()


def test_get_conn_returns_new():
    """Test that get_conn returns a new connection the first time"""
    assert False


def test_get_conn_returns_existing():
    """Test that get_conn returns an existing one if already created"""
    assert False


def test_get_conn_throws():
    """Test that get_conn throws if host, jwt_url or jwt_token_path are not present"""
    assert False
