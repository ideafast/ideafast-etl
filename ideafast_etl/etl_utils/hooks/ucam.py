from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import requests
from etl_utils.hooks.jwt import JwtHook

CURRENT_DIR = Path(__file__).parent


class DiseaseType(Enum):
    """Fixed and known disease groups"""

    Healthy = 1  #
    HD = 2  # Huntington's
    IBD = 3  # Inflammatory bowel
    PD = 4  # Parkinson's
    PSS = 5  # Progressive systemic sclerosis
    RA = 6  # Rheumatoid arthritis
    SLE = 7  # Systemic lupus erythematosus


@dataclass
class Patient:
    """Patient class for parsing UCAM data"""

    patient_id: str
    disease: DiseaseType
    start_wear: datetime
    end_wear: Optional[datetime]
    deviations: Optional[str]
    vttsma_id: Optional[str]
    dmp_dataset: Optional[str]

    @classmethod
    def serialize(cls, payload: dict) -> Patient:
        """Parse UCAM data and return a Patient object"""
        return cls(
            start_wear=cls.format_weartime(payload["start_Date"]),
            end_wear=cls.format_weartime(payload["end_Date"])
            if payload["end_Date"]
            else None,
            deviations=payload["deviations"],
            vttsma_id=payload["vtT_id"],
            patient_id=payload["subject_id"],
            disease=DiseaseType(int(payload["subject_Group"])),
            # TODO: adjust with UCAM database code
            dmp_dataset=None,
        )

    @staticmethod
    def format_weartime(time: str) -> datetime:
        """Create a datetime object from a UCAM provide weartime string"""
        return datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")


@dataclass
class Device:
    """Device class for parsing UCAM data"""

    device_id: str
    patients: List[Patient]

    @classmethod
    def serialize(cls, payload: dict) -> Device:
        """Parse UCAM data and return a Device object"""
        return cls(
            device_id=payload["device_id"],
            patients=[Patient.serialize(patients) for patients in payload["patients"]],
        )


class UcamHook(JwtHook):
    """Hook for interfacing with the JWT REST APIs from UCAM (Cambridge Uni)"""

    default_conn_name = "ucam_default"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        """Init a JwtHook with overriden default connection name"""
        JwtHook.__init__(self, conn_id=conn_id)

    def _jwt_prepared_request(self) -> requests.PreparedRequest:
        """Return a prepared JWT requests specific to UCAM"""
        return requests.Request(
            "POST", self.jwt_url, json={"Username": self.user, "Password": self.passw}
        ).prepare()

    def resolve_patient_id(
        self, device_id: str, start_wear: datetime, end_wear: datetime
    ) -> Optional[str]:
        """Resolve a device ID to a patient_id based on the assigned wear period"""
        start_wear = self.normalise_day(start_wear)
        end_wear = self.normalise_day(end_wear)
        device = self.get_device(device_id)

        return (
            self.get_patient_by_wear_period(device.patients, start_wear, end_wear)
            if device
            else None
        )

    def get_device(self, device_id: str) -> Optional[Device]:
        """Retrieve a device from the UCAM DB"""
        session = self.get_conn()

        url = self.base_url + f"devices/{device_id}"

        response = session.get(url)
        response.raise_for_status()
        result: dict = response.json()

        return Device.serialize(result[0]) if result else None

    def get_patient_by_wear_period(
        self,
        patients: List[Patient],
        start_wear: datetime,
        end_wear: datetime,
    ) -> Optional[str]:
        """Return patient_id by wear period"""
        for patient in patients:
            patient_start = self.normalise_day(patient.start_wear)
            # if end_wear is none, use today
            patient_end = self.normalise_day(patient.end_wear or datetime.today())

            within_start_period = patient_start <= start_wear <= patient_end
            within_end_period = patient_start <= end_wear <= patient_end

            if within_start_period and within_end_period:
                return patient.patient_id
        return None

    @lru_cache(maxsize=None)
    def _csv_as_dict(self, path: Path) -> Dict[str, str]:
        """
        Load full CSV as dict into memory for quick lookup.

        Assumes csv rows are unique.

        Note
        ----
        This needs updating after Dreem updated their data upload approach.
        Currently does a local lookup from a csv
        """
        with open(path, mode="r") as file:
            data = {rows[0]: rows[1] for rows in csv.reader(file)}

        return data

    def dreem_uid_to_serial(self, device_uid: str) -> Optional[str]:
        """
        Resolve a Dreem device UID to a device serial

        Note
        ----
        This needs updating after Dreem updated their data upload approach.
        Currently does a local lookup from a csv

        Parameters
        ----------
        device_uid : str
            The uid as assigned by the Dreem api from the recording.

        """
        data = self._csv_as_dict(CURRENT_DIR.parent / "dummy/dreem_uid_to_serial.csv")
        return data.get(device_uid)

    def serial_to_id(self, serial: str) -> Optional[str]:
        """
        Resolve any device serial to an IDEA-FAST device ID

        Note
        ----
        Needs to be reimplemented once UCAM integrates this service
        Currently does a local lookup from a csv

        Parameters
        ----------
        serial : str
            The serial of the device, as taken from the physical device itself
        """
        data = self._csv_as_dict(CURRENT_DIR.parent / "dummy/serial_to_id.csv")
        return data.get(serial)

    @staticmethod
    def normalise_day(_datetime: datetime) -> datetime:
        """Normalise a daytime to 00:00:00 for comparison"""
        return _datetime.replace(hour=0, minute=0, second=0, microsecond=0)
