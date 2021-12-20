from dataclasses import fields
from datetime import datetime
from unittest.mock import ANY, Mock

import pytest

from ideafast_etl.hooks.db import DeviceType, Record
from ideafast_etl.operators.ucam import GroupRecordsOperator, ResolveDeviceIdOperator

device_type = DeviceType.BTF


def test_ucam_no_records(mock_mongo_op, mock_ucam_op):
    """Test that nothing happens when no new records are found"""
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
    """Test that only the resolved records are updated"""
    mock_mongo_op().__enter__().find_deviceid_is_none.return_value = iter(
        ["test1", "test2"]
    )
    mock_ucam_op().__enter__().serial_to_id.side_effect = [99, None]

    task = ResolveDeviceIdOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    mock_mongo_op().__enter__().update_many_serial_to_deviceid.assert_called_once_with(
        "test1", 99, device_type
    )
    assert mock_ucam_op().__enter__().serial_to_id.call_count == 2


def test_mongo_grouping(mock_mongo_op):
    """Test that each collected record for grouping is handled"""
    loops = 5
    mock_mongo_op().__enter__().find_dmpid_is_none.return_value = [
        Mock(spec=[f.name for f in fields(Record)]) for _ in range(loops)
    ]

    task = GroupRecordsOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    assert mock_mongo_op().__enter__().custom_update_one.call_count == loops


@pytest.mark.parametrize(
    "start",
    [
        ("2021-11-29 00:00:00"),
        ("2021-11-29 00:00:01"),
        ("2021-11-29 23:59:59"),
    ],
)
def test_mongo_group_midnight(mock_mongo_op, start):
    """Test that a midnight cut_off time generates a 'TODAY-TODAY' dmp ID"""
    mock_record = Mock(spec=[f.name for f in fields(Record)])
    mock_record.start = datetime.fromisoformat(start)
    mock_record.device_id = "dummy-1"
    mock_record.patient_id = "dummy-1"
    mock_mongo_op().__enter__().find_dmpid_is_none.return_value = [mock_record]

    task = GroupRecordsOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    mock_mongo_op().__enter__().custom_update_one.assert_called_once_with(
        ANY, {"dmp_id": f"dummy1-dummy1-20211129-20211129"}
    )


@pytest.mark.parametrize(
    "input,expected",
    [
        ("2021-11-29 00:00:00", "dummy1-dummy1-20211129-20211129"),
        ("2021-11-29 00:00:01", "dummy1-dummy1-20211129-20211129"),
        ("2021-11-29 23:59:59", "dummy1-dummy1-20211129-20211129"),
    ],
)
def test_mongo_group_cut_off_default(mock_mongo_op, input, expected):
    """Test that a midnight cut_off time generates a 'TODAY-TODAY' dmp ID"""
    mock_record = Mock(spec=[f.name for f in fields(Record)])
    mock_record.start = datetime.fromisoformat(input)
    mock_record.device_id = "dummy-1"
    mock_record.patient_id = "dummy-1"
    mock_mongo_op().__enter__().find_dmpid_is_none.return_value = [mock_record]

    task = GroupRecordsOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result
    mock_mongo_op().__enter__().custom_update_one.assert_called_once_with(
        ANY, {"dmp_id": expected}
    )


@pytest.mark.parametrize(
    "time_input, cut_off_input, expected",
    [
        ("2021-11-29 08:59:59", "09:00:00", "dummy1-dummy1-20211128-20211129"),
        ("2021-11-29 09:00:00", "09:00:00", "dummy1-dummy1-20211129-20211130"),
        ("2021-11-29 09:00:01", "09:00:00", "dummy1-dummy1-20211129-20211130"),
    ],
)
def test_mongo_group_cut_off_custom(mock_mongo_op, time_input, cut_off_input, expected):
    """Test that a custom cut_off time generates the appropriate 2 day-window dmp ID"""
    mock_record = Mock(spec=[f.name for f in fields(Record)])
    mock_record.start = datetime.fromisoformat(time_input)
    mock_record.device_id = "dummy-1"
    mock_record.patient_id = "dummy-1"
    mock_mongo_op().__enter__().find_dmpid_is_none.return_value = [mock_record]

    task = GroupRecordsOperator(
        task_id="test", device_type=device_type, cut_off_time=cut_off_input
    )

    result = task.execute(context={})

    assert result
    mock_mongo_op().__enter__().custom_update_one.assert_called_once_with(
        ANY, {"dmp_id": expected}
    )


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
def test_mongo_grouping_time_formatting(mock_mongo_op, time):

    result = GroupRecordsOperator(
        task_id="test", device_type=device_type, cut_off_time=time
    )

    assert result
