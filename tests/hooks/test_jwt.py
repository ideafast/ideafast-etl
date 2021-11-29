import json
from unittest.mock import patch

import pytest
from airflow.models import Connection

from ideafast_etl.hooks.jwt import JwtHook


@pytest.fixture(scope="module")
def mock_connection():
    """Return a test Connection for each Hook instance"""
    extras = json.dumps(
        {
            "jwt_token": "test_jwt_token",
            "jwt_token_path": "test.token.path",
            "jwt_url": "test_jwt_url",
        }
    )
    test_connection = Connection(
        conn_id="test_conn",
        host="test_host",
        login="test_login",
        password="test_passw",
        extra=extras,
    )
    with patch.object(JwtHook, "get_connection", test_connection) as mock_connection:
        yield mock_connection


@pytest.fixture(scope="module")
def haystack():
    """Return a haystack and needle for pathfinding"""
    needle = "needle"
    payload = {
        "haystack1": {"haystack2": {"haystack3": None, "haystack4": needle}},
        "haystack5": [{"haystack6": None}, {"haystack7": needle}],
        "haystack4": "false copy",
    }
    return (payload, needle)


def test_find_jwt_token(haystack):
    """Test that a deep token can be found"""
    path = "haystack1.haystack2.haystack4"
    jwt_hook = JwtHook()

    result = jwt_hook._find_jwt_token(path, haystack[0])

    assert result == haystack[1]


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


def test_find_not_existing_jwt_token():
    """Test that a deep token can _not_ be found"""
    # should raise KeyError
    assert False


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    assert False


def test_jwt_get_token_still_valid():
    """Test that a jwt_token is returned if still valid"""
    assert False


def test_jwt_get_token_not_valid():
    """Test that a jwt_token is refreshed if no longer valid"""
    assert False


def test_jwt_get_token_storing():
    """Test that a jwt_token is stored in the Connections list"""
    assert False


def test_get_conn_returns_new():
    """Test that get_conn returns a new connection the first time"""
    assert False


def test_get_conn_returns_existing():
    """Test that get_conn returns an existing one if already created"""
    assert False


def test_get_conn_throws():
    """Test that get_conn throws if host, jwt_url or jwt_token_path are not present"""
    assert False
