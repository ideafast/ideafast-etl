import hashlib
import warnings
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Set

from airflow.providers.mongo.hooks.mongo import MongoHook
from bson import ObjectId

from ideafast_etl.hooks.db import DeviceType

DEFAULTS = {"mongo_collection": "etl_monitoring"}


@dataclass
class DeviceMonitorRecord:
    """Sub ETL stats record for individual pipelines"""

    total_db_records: int
    not_uploaded: int
    unique_no_device_id: int = field(init=False)
    unique_no_patient_id: int = field(init=False)
    unique_no_dmp_dataset_id: int = field(init=False)
    unique_not_uploaded: int = field(init=False)

    list_no_device_id: List[str]
    list_no_patient_id: List[str]
    list_no_dmp_dataset: List[str]
    list_not_uploaded: List[str]

    def __post_init__(self):
        """Calculate and store counts of 'stuck' records"""
        self.unique_no_device_id = len(self.list_no_device_id)
        self.unique_no_patient_id = len(self.list_no_patient_id)
        self.unique_no_dmp_dataset_id = len(self.list_no_dmp_dataset)
        self.unique_not_uploaded = len(self.list_not_uploaded)


@dataclass
class MonitoringRecord:
    """1-2-1 mapping of database records, with default initialisers"""

    date: datetime
    pipeline_stats: Dict[str, DeviceMonitorRecord]
    _id: Optional[ObjectId] = None

    @property
    def as_db_dict(self) -> dict:
        """Convert the dataclass to dict for inserting into MongoDB"""
        result = asdict(self)
        result.pop("_id")
        return result


class LocalMonitorMongoHook(MongoHook):
    """
    Hook extending MongoHook for interfacing with local MongoDB

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def custom_insert_one(self, record: MonitoringRecord) -> ObjectId:
        """Insert one record into the DB, return the ID"""
        result = self.insert_one(**DEFAULTS, doc=record.as_db_dict)
        return result.inserted_id

    def get_latest_stats(self) -> Optional[MonitoringRecord]:
        """Get the latest stats on the metadb"""
        result = self.find(**DEFAULTS, query={}, find_one=True, sort=[("date", -1)])
        return MonitoringRecord(**result) if result else None

    def get_stats_by_pipeline(start: datetime, end: datetime):
        pass

    # def __custom_find(self, filter: dict) -> Generator[Record, None, None]:
    #     """Get all (full) records with given filter"""
    #     result = self.find(**DEFAULTS, query=filter)
    #     yield from (Record(**r) for r in result)
