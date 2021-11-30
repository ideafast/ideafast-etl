from unittest.mock import patch

import pytest
from airflow.models import Connection

from ideafast_etl.hooks.jwt import JwtHook


@pytest.fixture(scope="module")
def mock_jwthook_get_connection():
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


@pytest.fixture()
def mock_requests():
    """Return a mocked requests library"""
    with patch("ideafast_etl.hooks.jwt.requests") as mock_request:
        yield mock_request


@pytest.fixture()
def mock_airflow_settings():
    """Return a mocked airflow settings object"""
    with patch("ideafast_etl.hooks.jwt.settings") as mock_settings:
        yield mock_settings
