from datetime import datetime

import pytest
import requests

from ideafast_etl.hooks.ucam import UcamHook


def test_resolve_patient_not_device():
    """Test that the method returns None if the device is not found"""
    pass


def test_resolve_patient_normalises_wears():
    """Test that the method uses normalised days for comparison"""
    pass


def test_get_device_found():
    """Test that the device is found if in expected payload"""
    pass


def test_get_device_not_found():
    """Test that the device is NOT found if in expected payload"""
    pass


def test_get_patient_by_wear_within():
    """Test that a patient is found if within the wear period"""
    pass


def test_get_patient_by_wear_within_no_endwear():
    """Test that a patient is found if within the wear period despite no endwear"""
    pass


def test_get_patient_by_wear_partially_outside():
    """Test that patient is not found, because recording-end or start are not within"""
    pass


def test_get_patient_by_wear_outside():
    """Test that patient is not found, because recording is outside of period"""
    pass


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
