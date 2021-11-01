from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

from airflow.providers.mongo.hooks.mongo import MongoHook
from bson import ObjectId

DEFAULTS = {"mongo_collection": "ideafast_etl"}


class DeviceType(Enum):
    """Devices used in IDEAFAST, FS and COS"""

    AX6 = 1  # Axivity
    BTF = 2  # Byteflies
    DRM = 3  # Dreem
    TFA = 4  # Think Fast
    BVN = 5  # Everion
    MMM = 6  # Move Monitor
    SMP = 7  # Samsung Smartphone
    SMA = 8  # Stress Monitor App
    BED = 9  # EBedSensor
    VTP = 10  # Vital Patch
    YSM = 11  # ZKOne YOLI


@dataclass
class Record:
    """1-2-1 mapping of database records, with default initialisers"""

    _id: Optional[ObjectId]
    hash: str
    manufacturer_ref: str
    device_type: str
    start_wear: datetime
    end_wear: datetime
    meta: dict = field(default_factory=dict)  # additional details if needed

    device_id: Optional[str] = None
    patient_id: Optional[str] = None
    dmp_id: Optional[str] = None


def create_record(record: Record) -> ObjectId:
    """Insert one record into the DB, return the ID"""
    with MongoHook() as db:
        result = db.insert_one(**DEFAULTS, doc=asdict(record))
        return result.inserted_id


def read_record(record_id: str) -> Record:
    """Read one record from the DB using its ID"""
    with MongoHook() as db:
        result = db.find(**DEFAULTS, query={"_id": ObjectId(record_id)}, find_one=True)
        return Record(**result)


def update_record(record: Record) -> bool:
    """Update one record from the DB"""
    with MongoHook() as db:
        result = db.update_one(
            **DEFAULTS,
            filter_doc={"_id": record._id},
            update_doc={"$set": asdict(record)},
        )
        return result.modified_count == 1


def _get_records(filter: dict) -> List[Record]:
    """Get all (full) records with given filter"""
    with MongoHook() as db:
        result = db.find(**DEFAULTS, query=filter)
        return [Record(**r) for r in result]


def get_unresolved_device_records(device_type: DeviceType) -> List[Record]:
    """Get all records from a specific devicetype without device IDs"""
    return _get_records(filter={"device_type": device_type.value, "device_id": None})


def get_unresolved_patient_records(device_type: DeviceType) -> List[Record]:
    """Get all records from a specific devicetype without patient IDs"""
    return _get_records(filter={"device_type": device_type.value, "patient_id": None})


def get_unprocessed_records(device_type: DeviceType) -> List[Record]:
    """
    Get all records from a specific device type that have not been downloaded
    and uploaded to the DMP yet"""
    return _get_records(filter={"device_type": device_type.value, "dmp_id": None})


def get_list_of_hashes(device_type: DeviceType) -> List[str]:
    """Get all hash representations of stored files"""
    with MongoHook() as db:
        result = db.find(
            **DEFAULTS, query={"device_type": device_type.value}, projection=["hash"]
        )
        return [r["hash"] for r in result]
