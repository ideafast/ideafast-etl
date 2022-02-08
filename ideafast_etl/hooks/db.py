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
    dmp_dataset: Optional[str] = None
    dmp_id: Optional[str] = None
    is_uploaded: bool = False

    @staticmethod
    def generate_hash(input: str, device_type: DeviceType) -> str:
        """Generate a unique hash for DB comparison across devices"""
        result = hashlib.sha256()
        result.update(device_type.name.encode("utf-8"))
        result.update(input.encode("utf-8"))
        return result.hexdigest()

    @property
    def as_db_dict(self) -> dict:
        """Convert the dataclass to dict for inserting into MongoDB"""
        result = asdict(self)
        result.pop("_id")
        result.update(device_type=self.device_type.name)
        return result


class LocalMongoHook(MongoHook):
    """
    Hook extending MongoHook for interfacing with local MongoDB

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def custom_insert_one(self, record: Record) -> ObjectId:
        """Insert one record into the DB, return the ID"""
        result = self.insert_one(**DEFAULTS, doc=record.as_db_dict)
        return result.inserted_id

    def custom_insert_many(self, records: List[Record]) -> List[ObjectId]:
        """Insert multiple records into the DB, return the ID"""
        result = self.insert_many(
            **DEFAULTS, docs=[r.as_db_dict for r in records], ordered=False
        )
        return result.inserted_ids

    def custom_update_one(self, record_id: ObjectId, changes: Dict[str, Any]) -> bool:
        """Update one record from the DB with the given updates to do"""
        result = self.update_one(
            **DEFAULTS,
            filter_doc={"_id": record_id},
            update_doc={"$set": changes},
        )
        return result.modified_count == 1

    def update_drmuid_to_serial(self, uid: str, serial: str) -> int:
        """Update many DRM record's device_serials upon resolving a DRM uid"""
        # NOTE: Currently ignores records that already had a serial set to avoid overrides
        result = self.update_many(
            **DEFAULTS,
            filter_doc={
                "device_type": DeviceType.DRM.name,
                "meta.dreem_uid": uid,
                "device_serial": None,
            },
            update_doc={"$set": {"device_serial": serial}},
        )
        return result.modified_count

    def update_many_serial_to_deviceid(
        self, serial: str, device_id: str, device_type: DeviceType
    ) -> int:
        """Update many record's device_ids upon resolving the device_serials"""
        # NOTE: Currently ignores records that already had a device_id set to avoid overrides
        result = self.update_many(
            **DEFAULTS,
            filter_doc={
                "device_type": device_type.name,
                "device_serial": serial,
                "device_id": None,
            },
            update_doc={"$set": {"device_id": device_id}},
        )
        return result.modified_count

    def __custom_delete_one(self, record_id: str) -> bool:
        """Delete one record from the DB using its ID"""
        warnings.warn("Never use this method within the pipeline", UserWarning)
        result = self.delete_one(
            **DEFAULTS, query={"_id": ObjectId(record_id)}, find_one=True
        )
        return result.deleted_count == 1

    def __custom_find(self, filter: dict) -> Generator[Record, None, None]:
        """Get all (full) records with given filter"""
        result = self.find(**DEFAULTS, query=filter)
        yield from (Record(**r) for r in result)

    def find_deviceid_is_none(
        self,
        device_type: DeviceType,
    ) -> Generator[str, None, None]:
        """Retrieve a reduced but full set of unique device serials to resolve"""
        seen = set()
        for record in self.__custom_find(
            # device_serial must not be none
            filter={
                "device_type": device_type.name,
                "device_serial": {"$ne": None},
                "device_id": None,
            }
        ):
            if (serial := record.device_serial) not in seen:
                yield serial
                seen.add(serial)

    def find_drm_deviceserial_is_none(self) -> Generator[str, None, None]:
        """Retrieve a reduced but full set of unique dreem uids to resolve"""
        seen = set()
        for record in self.__custom_find(
            filter={
                "device_type": DeviceType.DRM.name,
                "meta.dreem_uid": {"$ne": "null"},
                "device_serial": None,
            },
        ):
            if (uid := record.meta.get("dreem_uid")) not in seen:
                yield uid
                seen.add(uid)

    def find_patientid_is_none(
        self,
        device_type: DeviceType,
    ) -> Generator[Record, None, None]:
        """Get all records from a specific devicetype without patient_id, with device_id"""
        yield from self.__custom_find(
            filter={
                "device_type": device_type.name,
                "device_id": {"$ne": None},
                "patient_id": None,
            }
        )

    def find_records_by_dmpid(self, dmp_id: str) -> List[Record]:
        """Get all records from a specific dmp_id"""
        return list(self.__custom_find(filter={"dmp_id": dmp_id}))

    def update_dmpid_records_uploaded(self, dmp_id: str) -> int:
        """Update all records 'is_uploaded' with the same dmp_id"""
        result = self.update_many(
            **DEFAULTS,
            filter_doc={"dmp_id": dmp_id},
            update_doc={"$set": {"is_uploaded": True}},
        )
        return result.modified_count

    def find_dmpid_is_none(self, device_type: DeviceType) -> List[Record]:
        """Get all records from a specific device type without dmp_id, with patient_id"""
        # Returns a list, as we need to handle everything we have to avoid data gaps
        return list(
            self.__custom_find(
                filter={
                    "device_type": device_type.name,
                    "patient_id": {"$ne": None},
                    "dmp_id": None,
                }
            )
        )

    def find_notuploaded_dmpids(
        self,
        device_type: DeviceType,
    ) -> Generator[str, None, None]:
        """Get all unique dmp_ids that still have unfinished uploads"""
        seen = set()
        for record in self.__custom_find(
            filter={
                "device_type": device_type.name,
                "dmp_id": {"$ne": None},
                "is_uploaded": False,
            }
        ):
            if (dmp_id := record.dmp_id) not in seen:
                yield dmp_id
                seen.add(dmp_id)

    def find_hashes(self, device_type: DeviceType) -> Set[str]:
        """Get all hash representations of stored files"""
        result = self.__custom_find(filter={"device_type": device_type.name})
        return {r.hash for r in result}

    def query_stats(self, query: list) -> List[dict]:
        """Query the records DB for sensor data progress"""
        return self.aggregate(**DEFAULTS, aggregate_query=query)
