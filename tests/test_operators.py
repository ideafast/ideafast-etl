from unittest.mock import patch

from ideafast_etl.hooks.db import DeviceType
from ideafast_etl.operators.ucam import ResolveDeviceIdOperator


# always patch from where the item is imported
@patch("ideafast_etl.operators.ucam.UcamHook")
@patch("ideafast_etl.operators.ucam.LocalMongoHook")
def test_ucam_operator(mock_mongo, mock_ucam):

    device_type = DeviceType.BTF

    mock_mongo().__enter__().find_deviceid_is_none.return_value = iter(
        ["test_serial1", "test_serial2"]
    )
    mock_mongo().__enter__().update_many_serial_to_deviceid.return_value = 99
    mock_ucam().__enter__().serial_to_id.return_value = "test_id"

    task = ResolveDeviceIdOperator(task_id="test", device_type=device_type)

    result = task.execute(context={})

    assert result

    mock_mongo().__enter__().find_deviceid_is_none.assert_called_once_with(device_type)
    mock_ucam().__enter__().serial_to_id.called_with("test_serial2")
    mock_mongo().__enter__().update_many_serial_to_deviceid.call_count == 2
    mock_mongo().__enter__().update_many_serial_to_deviceid.called_with(
        "test_serial2", 99, device_type
    )
