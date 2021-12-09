from datetime import datetime
from unittest.mock import patch

import pytest
import requests

from ideafast_etl.hooks.ucam import UcamHook


def test_resolve_patient_not_device():
    """Test that the method returns None if the device is not found"""

    assert False


def test_resolve_patient_normalises_wears():
    """Test that the method uses normalised days for comparison"""

    assert False


def test_get_device_found(
    test_connection,
    mock_requests_ucam,
):
    """Test that the device is found if in expected payload"""

    with patch.object(UcamHook, "get_connection", return_value=test_connection):
        ucam_hook = UcamHook()

        result = ucam_hook.get_device("NR1_DEVICE")

        assert result.device_id == "NR1_DEVICE"


def test_get_device_not_found(
    test_connection,
    mock_requests_ucam,
):
    """Test that the device is found if in expected payload"""
    mock_requests_ucam.Session().get.return_value.json.return_value = []
    with patch.object(UcamHook, "get_connection", return_value=test_connection):
        ucam_hook = UcamHook()

        result = ucam_hook.get_device("NOT_DEVICE")

        assert result is None


def test_get_patient_by_wear_within():
    """Test that a patient is found if within the wear period"""

    assert False


def test_get_patient_by_wear_within_no_endwear():
    """Test that a patient is found if within the wear period despite no endwear"""

    assert False


def test_get_patient_by_wear_partially_outside():
    """Test that patient is not found, because recording-end or start are not within"""

    assert False


def test_get_patient_by_wear_outside():
    """Test that patient is not found, because recording is outside of period"""

    assert False


@pytest.mark.parametrize(
    "date1, date2",
    [
        ([2021, 3, 27, 0, 0, 0, 647241], [2021, 3, 27, 0, 0, 0, 565704]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 0, 1, 0]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 1, 0, 0]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 1, 0, 0, 0]),
    ],
)
def test_normalise_day_true(date1, date2) -> None:
    """Test that normalise day returns equal days in all cases"""
    one = UcamHook.normalise_day(datetime(*date1))
    two = UcamHook.normalise_day(datetime(*date2))

    result = one == two

    assert result


@pytest.mark.parametrize(
    "date1, date2",
    [
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 26, 0, 0, 0, 0]),
        ([2022, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 0, 0, 0]),
        ([2021, 4, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 0, 0, 0]),
    ],
)
def test_normalise_day_false(date1, date2) -> None:
    """Test that normalise day returns inequal days in all cases"""
    one = UcamHook.normalise_day(datetime(*date1))
    two = UcamHook.normalise_day(datetime(*date2))

    result = one != two

    assert result


def test_jwt_prepared_request():
    """Test that the prepare method returns a PREPARED request"""
    ucam_hook = UcamHook()
    ucam_hook.jwt_url = "http://test"
    ucam_hook.user = "test_user"
    ucam_hook.passw = "test_passw"

    result = ucam_hook._jwt_prepared_request()

    assert isinstance(result, requests.PreparedRequest)
