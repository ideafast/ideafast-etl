from datetime import datetime
from unittest.mock import ANY, patch

import pytest
import requests

from ideafast_etl.hooks.ucam import UcamHook


def test_resolve_patient_not_device(
    test_connection,
    mock_requests_ucam,
    mock_get_connection,
):
    """Test that the method returns None if the device is not found"""
    # override return value
    mock_requests_ucam.Session().get.return_value.json.return_value = []

    ucam_hook = UcamHook()

    result = ucam_hook.resolve_patient_id(
        "NOT_DEVICE",
        datetime(2021, 11, 4, 12, 0, 0),
        datetime(2021, 11, 5, 12, 0, 0),
    )

    assert result is None


def test_resolve_patient_normalises_wears(
    test_connection,
    mock_requests_ucam,
    mock_get_connection,
):
    """Test that the method uses normalised days for comparison"""
    equal_date = datetime(2021, 11, 4, 0, 0, 0, 0)
    ucam_hook = UcamHook()
    with patch.object(
        ucam_hook, "get_patient_by_wear_period", return_value=None
    ) as mocked_wear:
        print(ucam_hook.get_patient_by_wear_period)

        result = ucam_hook.resolve_patient_id(
            "NR1_DEVICE",
            datetime(2021, 11, 4, 12, 3, 55),
            datetime(2021, 11, 4, 23, 9, 2),
        )

        print(ucam_hook.get_patient_by_wear_period.call_args)
        ucam_hook.get_patient_by_wear_period.assert_called_once_with(
            ANY, equal_date, equal_date
        )


def test_get_device_found(
    test_connection,
    mock_requests_ucam,
    mock_get_connection,
):
    """Test that the device is found if in expected payload"""
    ucam_hook = UcamHook()

    result = ucam_hook.get_device("NR1_DEVICE")

    assert result.device_id == "NR1_DEVICE"


def test_get_device_not_found(test_connection, mock_requests_ucam, mock_get_connection):
    """Test that the device is not found if in expected payload"""
    # override return value
    mock_requests_ucam.Session().get.return_value.json.return_value = []

    ucam_hook = UcamHook()

    result = ucam_hook.get_device("NOT_DEVICE")

    assert result is None


# TODO: Discuss what behaviour we want when timings cross boundaries?...
@pytest.mark.parametrize(
    "date1, date2, expected",
    [
        ([2021, 11, 4, 12, 0, 0], [2021, 11, 5, 12, 0, 0], "B-PATIENT"),
        ([2021, 11, 6, 22, 0, 0], [2021, 11, 6, 23, 0, 0], "C-PATIENT"),
        ([2021, 11, 13, 0, 0, 0], [2021, 11, 14, 0, 0, 0], "A-PATIENT"),
        ([2021, 11, 1, 0, 0, 0], [2021, 11, 1, 0, 0, 0], None),
        ([2021, 11, 3, 0, 0, 0], [2021, 11, 4, 12, 0, 0], None),
        ([2021, 11, 6, 13, 0, 0], [2021, 11, 8, 0, 0, 0], None),
    ],
    ids=[
        "within",
        "within evening",
        "within no end time",
        "outside",
        "outside early start",
        "outside late end",
    ],
)
def test_get_patient_by_wear_within(
    date1, date2, expected, test_connection, mock_requests_ucam, mock_get_connection
):
    """Test that a patient is correctly found (or not) if within the wear period, even without endwear"""
    start_wear, end_wear = datetime(*date1), datetime(*date2)
    ucam_hook = UcamHook()
    patients = ucam_hook.get_device("NR1_DEVICE").patients

    result = ucam_hook.get_patient_by_wear_period(patients, start_wear, end_wear)

    assert result == expected


@pytest.mark.parametrize(
    "date1, date2",
    [
        ([2021, 3, 27, 0, 0, 0, 647241], [2021, 3, 27, 0, 0, 0, 565704]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 0, 1, 0]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 0, 1, 0, 0]),
        ([2021, 3, 27, 0, 0, 0, 0], [2021, 3, 27, 1, 0, 0, 0]),
    ],
)
def test_normalise_day_true(date1, date2):
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
