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


@lru_cache
def dreem_uid_to_serial(device_uid: str) -> Optional[str]:
    """
    Resolves a Dreem device UID to a device serial

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


@lru_cache
def serial_to_id(serial: str) -> Optional[str]:
    """
    Resolves any device serial to an IDEA-FAST device ID

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
