import hashlib
import warnings
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Set

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
    device_type: DeviceType
    start: datetime
    end: datetime
    meta: dict = field(default_factory=dict)  # additional details if needed

    device_serial: Optional[str] = None
    device_id: Optional[str] = None
    patient_id: Optional[str] = None
    dmp_id: Optional[str] = None

    @staticmethod
    def generate_hash(input: str, device_type: DeviceType) -> str:
        """Generate a unique hash for DB comparison across devices"""
        result = hashlib.sha256()
        result.update(device_type.name.encode("utf-8"))
        result.update(input.encode("utf-8"))
        return result.hexdigest()

    def as_db_dict(self) -> dict:
        """Convert the dataclass to dict for inserting into MongoDB"""
        result = asdict(self)
        result.pop("_id")
        result.update(device_type=self.device_type.name)
        return result


def create_record(record: Record) -> ObjectId:
    """Insert one record into the DB, return the ID"""
    with MongoHook() as db:
        result = db.insert_one(**DEFAULTS, doc=record.as_db_dict())
        return result.inserted_id


def create_many_records(records: List[Record]) -> List[ObjectId]:
    """Insert multiple records into the DB, return the ID"""
    with MongoHook() as db:
        result = db.insert_many(
            **DEFAULTS, docs=[r.as_db_dict() for r in records], ordered=False
        )
        return result.inserted_ids


def read_record(record_id: str) -> Record:
    """Read one record from the DB using its ID"""
    with MongoHook() as db:
        result = db.find(**DEFAULTS, query={"_id": ObjectId(record_id)}, find_one=True)
        return Record(**result)


def update_record(record_id: ObjectId, changes: Dict[str, Any]) -> bool:
    """Update one record from the DB with the given updates to do"""
    with MongoHook() as db:
        result = db.update_one(
            **DEFAULTS,
            filter_doc={"_id": record_id},
            update_doc={"$set": changes},
        )
        return result.modified_count == 1


def update_many_drm_serials(uid: str, serial: str) -> int:
    """
    Update many DRM record's device_serials upon resolving a DRM uid

    Note
    ----
    Currently ignores records that already had a serial set to avoid overrides
    """
    with MongoHook() as db:
        result = db.update_many(
            **DEFAULTS,
            filter_doc={
                "device_type": DeviceType.DRM.name,
                "meta.dreem_uid": uid,
                "device_serial": None,
            },
            update_doc={"$set": {"device_serial": serial}},
        )
        return result.modified_count


def update_many_device_ids(serial: str, device_id: str, device_type: DeviceType) -> int:
    """
    Update many record's device_ids upon resolving the device_serials

    Note
    ----
    Currently ignores records that already had a device_id set to avoid overrides
    """
    with MongoHook() as db:
        result = db.update_many(
            **DEFAULTS,
            filter_doc={
                "device_type": device_type.name,
                "device_serial": serial,
                "device_id": None,
            },
            update_doc={"$set": {"device_id": device_id}},
        )
        return result.modified_count


def __delete_record(record_id: str) -> bool:
    """Delete one record from the DB using its ID"""
    warnings.warn("Never use this method within the pipeline", UserWarning)

    with MongoHook() as db:
        result = db.delete_one(
            **DEFAULTS, query={"_id": ObjectId(record_id)}, find_one=True
        )
        return result.deleted_count == 1


def _get_records(filter: dict) -> Generator[Record, None, None]:
    """Get all (full) records with given filter"""
    with MongoHook() as db:
        result = db.find(**DEFAULTS, query=filter)
        yield from (Record(**r) for r in result)


def _get_unresolved_device_serial_records(
    device_type: DeviceType,
) -> Generator[Record, None, None]:
    """Get all records from a specific devicetype without device serials"""
    yield from _get_records(
        filter={"device_type": device_type.name, "device_serial": None}
    )


def _get_unresolved_device_id_records(
    device_type: DeviceType,
) -> Generator[Record, None, None]:
    """Get all records from a specific devicetype without device IDs"""
    yield from _get_records(filter={"device_type": device_type.name, "device_id": None})


def get_unresolved_device_serials(
    device_type: DeviceType,
) -> Generator[str, None, None]:
    """Retrieve a reduced but full set of unique device serials to resolve"""
    seen = set()
    for record in _get_unresolved_device_id_records(device_type):
        if (serial := record.device_serial) and serial not in seen:
            yield serial
            seen.add(serial)


def get_unresolved_dreem_uids() -> Generator[str, None, None]:
    """Retrieve a reduced but full set of unique dreem uids to resolve"""
    seen = set()
    for record in _get_unresolved_device_serial_records(DeviceType.DRM):
        if (uid := record.meta.get("dreem_uid")) not in seen:
            yield uid
            seen.add(uid)


def get_unresolved_patient_records(
    device_type: DeviceType,
) -> Generator[Record, None, None]:
    """Get all records from a specific devicetype without patient_id, with device_id"""
    yield from _get_records(
        filter={
            "device_type": device_type.name,
            "device_id": {"$ne": "null"},
            "patient_id": None,
        }
    )


def get_unprocessed_records(device_type: DeviceType) -> Generator[Record, None, None]:
    """Get all records from a specific device type that have not been down/uploaded yet"""
    yield from _get_records(filter={"device_type": device_type.name, "dmp_id": None})


def get_hashes(device_type: DeviceType) -> Set[str]:
    """Get all hash representations of stored files"""
    with MongoHook() as db:
        result = db.find(
            **DEFAULTS, query={"device_type": device_type.name}, projection=["hash"]
        )
        return {r["hash"] for r in result}
