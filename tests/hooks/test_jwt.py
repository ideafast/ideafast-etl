import json
from unittest.mock import MagicMock

import jwt
import pytest
import requests

from ideafast_etl.hooks.jwt import JwtHook


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


def test_jwt_get_token_still_valid(mock_requests, new_jwt_key, mock_setting_session):
    """
    Test that a jwt_token is returned if still valid

    i.e., not making a HTTP request or storing a new value in Airflow
    """
    jwt_hook = JwtHook()
    jwt_hook.jwt_token = new_jwt_key

    result = jwt_hook._get_jwt_token()

    assert result == new_jwt_key
    assert not mock_requests.called
    assert not mock_setting_session.called


def test_jwt_get_token_not_valid(mock_requests, old_jwt_key, mock_setting_session):
    """Test that a jwt_token is refreshed if no longer valid, and stored into connections"""
    jwt_hook = JwtHook()
    jwt_hook.jwt_token = old_jwt_key
    jwt_hook.jwt_token_path = "jwt_token"

    result = jwt_hook._get_jwt_token()

    assert result != old_jwt_key
    mock_setting_session().commit.assert_called_once()


def test_get_conn_returns_new(mock_requests, mock_setting_session, mock_get_connection):
    """Test that get_conn returns a new connection the first time"""
    jwt_hook = JwtHook()

    result = jwt_hook.get_conn()

    assert result == mock_requests.Session()


def test_get_conn_returns_existing(
    mock_requests, mock_setting_session, mock_get_connection
):
    """Test that get_conn returns an existing one if already created"""
    jwt_hook = JwtHook()
    jwt_hook.get_conn()  # received mock_requests
    jwt_hook.session = MagicMock()  # override with new mock

    result = jwt_hook.get_conn()  # should retunr overrided mock, not new one

    assert result is not mock_requests.Session()


def test_get_conn_throws():
    """Test that get_conn throws if host, jwt_url or jwt_token_path are not present"""
    assert False
