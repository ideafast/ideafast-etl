import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
import pytest
from airflow.models import Connection

from ideafast_etl.hooks.jwt import JwtHook


@pytest.fixture(scope="module")
def mock_get_connection():
    """Return a test Connection for each Hook instance"""
    extras = json.dumps(
        {
            "jwt_token_path": "jwt_token",
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
    with patch.object(
        JwtHook, "get_connection", return_value=test_connection
    ) as mock_connection:
        yield mock_connection


@pytest.fixture()
def old_jwt_key():
    """Return a mocked requests library"""
    yield jwt.encode(
        {"exp": datetime.now(tz=timezone.utc) - timedelta(seconds=30)}, "secret"
    ).decode("utf-8")


@pytest.fixture()
def new_jwt_key():
    """Return a mocked requests library"""
    yield jwt.encode(
        {"exp": datetime.now(tz=timezone.utc) + timedelta(minutes=30)}, "secret"
    ).decode("utf-8")


@pytest.fixture()
def mock_requests(new_jwt_key):
    """Return a mocked requests library"""
    with patch("ideafast_etl.hooks.jwt.requests") as mock_request:
        mock_request.Session().send.return_value.json.return_value = {
            "jwt_token": new_jwt_key
        }
        yield mock_request


@pytest.fixture()
def mock_setting_session():
    """Return a mocked airflow settings object"""
    with patch("ideafast_etl.hooks.jwt.settings.Session") as mock_session:
        mock_session.return_value.query.return_value.filter.return_value.one.return_value.extra = (
            "{}"
        )
        yield mock_session


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
