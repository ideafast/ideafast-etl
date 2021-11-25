from unittest.mock import Mock, patch

import pytest

from ideafast_etl.hooks.db import DeviceType
from ideafast_etl.operators.ucam import GroupRecordsOperator, ResolveDeviceIdOperator

device_type = DeviceType.BTF


def test_ucam_no_records(mock_mongo_op, mock_ucam_op):

    mock_mongo_op().__enter__().find_deviceid_is_none.return_value = iter([])
    mock_ucam_op().__enter__().serial_to_id.return_value = None

    task = ResolveDeviceIdOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    assert mock_mongo_op().__enter__().update_many_serial_to_deviceid.call_count == 0
    mock_mongo_op().__enter__().find_deviceid_is_none.assert_called_once_with(
        device_type
    )
    mock_ucam_op().__enter__().serial_to_id.assert_not_called()


def test_ucam_unresolved_id(mock_mongo_op, mock_ucam_op):
    mock_mongo_op().__enter__().find_deviceid_is_none.return_value = iter(
        ["test1", "test2"]
    )
    mock_ucam_op().__enter__().serial_to_id.side_effect = [99, None]

    task = ResolveDeviceIdOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    assert mock_mongo_op().__enter__().update_many_serial_to_deviceid.call_count == 1
    assert mock_ucam_op().__enter__().serial_to_id.call_count == 2


def test_mongo_grouping(mock_mongo_op):
    loops = 5
    mock_mongo_op().__enter__().find_dmpid_is_none.return_value = [
        Mock() for _ in range(loops)
    ]

    task = GroupRecordsOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    assert mock_mongo_op().__enter__().custom_update_one.call_count == loops


@pytest.mark.parametrize(
    "time",
    [
        pytest.param("9:09", marks=pytest.mark.xfail(raises=ValueError, strict=True)),
        pytest.param("9:09:", marks=pytest.mark.xfail(raises=ValueError, strict=True)),
        pytest.param(
            "9h 2m 5s", marks=pytest.mark.xfail(raises=ValueError, strict=True)
        ),
        pytest.param(
            "25:00:00", marks=pytest.mark.xfail(raises=ValueError, strict=True)
        ),
        pytest.param(
            "01:01:61", marks=pytest.mark.xfail(raises=ValueError, strict=True)
        ),
        ("01:10:01"),
        ("00:00:00"),
    ],
)
def test_mongo_grouping_faulty_time(mock_mongo_op, time):

    result = GroupRecordsOperator(
        task_id="test", device_type=device_type, cut_off_time=time
    )

    assert result
