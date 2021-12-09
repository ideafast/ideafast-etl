import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import jwt
import pytest
from airflow.models import Connection

from ideafast_etl.hooks.jwt import JwtHook
from ideafast_etl.hooks.ucam import Device


@pytest.fixture()
def old_jwt_key():
    """Return a expired JWT key"""
    return jwt.encode(
        {"exp": datetime.now(tz=timezone.utc) - timedelta(seconds=30)}, "secret"
    ).decode("utf-8")


@pytest.fixture()
def new_jwt_key():
    """Return a valid JWT key"""
    return jwt.encode(
        {"exp": datetime.now(tz=timezone.utc) + timedelta(minutes=30)}, "secret"
    ).decode("utf-8")


@pytest.fixture()
def connection_extras(new_jwt_key):
    return {
        "jwt_token_path": "jwt_token",
        "jwt_url": "test_jwt_url",
        "jwt_token": new_jwt_key,
    }


@pytest.fixture()
def connection_default_kwargs():
    return {
        "conn_id": "test_conn",
        "host": "test_host",
        "login": "test_login",
        "password": "test_passw",
    }


@pytest.fixture()
def mock_ucam_device_payload():
    """Return a mocked payload from UCAM API (v1 - FS)"""
    return [
        {
            "device_id": "NR1_DEVICE",
            "patients": [
                {
                    "subject_id": "A-PATIENT",
                    "subject_Group": 4,
                    "start_Date": "2021-11-10T00:00:00",
                    "end_Date": None,
                    "deviations": None,
                    "vtT_id": "vtt_test_id_A",
                },
                {
                    "subject_id": "B-PATIENT",
                    "subject_Group": 2,
                    "start_Date": "2021-11-04T00:00:00",
                    "end_Date": "2021-11-06T00:00:00",
                    "deviations": "sample deviation",
                    "vtT_id": "vtt_test_id_B",
                },
                {
                    "subject_id": "C-PATIENT",
                    "subject_Group": 1,
                    "start_Date": "2021-11-06T00:00:00",
                    "end_Date": "2021-11-07T00:00:00",
                    "deviations": "sample deviation 2",
                    "vtT_id": "vtt_test_id_C",
                },
            ],
        }
    ]


@pytest.fixture()
def test_connection(connection_extras, connection_default_kwargs):
    return Connection(
        **connection_default_kwargs,
        extra=json.dumps(connection_extras),
    )


@pytest.fixture()
def mock_requests(new_jwt_key):
    """Return a mocked requests library for the JWT hook"""
    with patch("ideafast_etl.hooks.jwt.requests") as mock_request:
        mock_request.Session().send.return_value.json.return_value = {
            "jwt_token": new_jwt_key
        }
        yield mock_request


@pytest.fixture()
def mock_requests_ucam(mock_ucam_device_payload):
    """Return a mocked requests library for the UCAM hook"""
    with patch("ideafast_etl.hooks.jwt.requests") as mock_request:
        mock_request.Session().get.return_value.json.return_value = (
            mock_ucam_device_payload
        )
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
