import csv
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

CURRENT_DIR = Path(__file__).parent


@lru_cache(maxsize=None)
def _csv_as_dict(path: Path) -> Dict[str, str]:
    """
    Load full CSV as dict into memory for quick lookup.

    Assumes csv rows are unique
    """
    with open(path, mode="r") as file:
        data = {rows[0]: rows[1] for rows in csv.reader(file)}

    return data


def dreem_uid_to_serial(device_uid: str) -> Optional[str]:
    """
    Resolve a Dreem device UID to a device serial

    NOTE
    ----
    This needs updating after Dreem updated their data upload approach.
    Currently does a local lookup from a csv

    Parameters
    ----------
    device_uid : str
        The uid as assigned by the Dreem api from the recording.

    """
    data = _csv_as_dict(CURRENT_DIR / "dummy/dreem_uid_to_serial.csv")
    return data.get(device_uid)


def serial_to_id(serial: str) -> Optional[str]:
    """
    Resolve any device serial to an IDEA-FAST device ID

    NOTE
    ----
    Needs to be reimplemented once UCAM integrates this service
    Currently does a local lookup from a csv

    Parameters
    ----------
    serial : str
        The serial of the device, as taken from the physical device itself
    """
    data = _csv_as_dict(CURRENT_DIR / "dummy/serial_to_id.csv")
    return data.get(serial)


# def patient_by_wear_period(
#     device_id: str, start_wear: datetime, end_wear: datetime
# ) -> Optional[str]:
#     """Resolve a device ID to a patient_id based on the assigned wear period"""
#     start_wear = _normalise_day(start_wear)
#     end_wear = _normalise_day(end_wear)
#     devices = get_one_device(device_id)

#     return determine_by_wear_period(devices, start_wear, end_wear)


# def _normalise_day(_datetime: datetime) -> datetime:
#     """Normalise a daytime to 00:00:00 for comparison"""
#     return _datetime.replace(hour=0, minute=0, second=0, microsecond=0)


# @lru_cache
# def get_device(device_id: str) -> Optional[List[DeviceWithPatients]]:
#     response = requests.get(f"{config.ucam_api}devices/{device_id}").json()
#     return (
#         [DeviceWithPatients.serialize(device) for device in response["data"]]
#         if response["meta"]["success"]
#         else None
#     )