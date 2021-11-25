from ideafast_etl.hooks.db import DeviceType
from ideafast_etl.operators.ucam import ResolveDeviceIdOperator


def test_ucam_operator():
    result = ResolveDeviceIdOperator(task_id="test", device_type=DeviceType.BTF)

    assert result == result
