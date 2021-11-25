import logging
from datetime import datetime, timedelta
from itertools import islice
from typing import Any, Optional

from airflow.models.baseoperator import BaseOperator

from ideafast_etl.hooks.db import DeviceType, LocalMongoHook
from ideafast_etl.hooks.ucam import UcamHook


class ResolveDeviceIdOperator(BaseOperator):
    """
    Retrieves records with unknown device IDs, and tries to resolve them

    Parameters
    ----------
    device_type: DeviceType
        the type of device to focus this operation on
    limit : None | int
        limit of how many device serials to handle this run - useful for testing
        or managing workload in batches
    """

    def __init__(
        self, device_type: DeviceType, limit: Optional[int] = None, **kwargs: Any
    ) -> None:
        """Construct Operator"""
        super().__init__(**kwargs)
        self.device_type = device_type
        self.limit = limit

    def execute(self, context: Any) -> bool:
        """Collect serials, and resolve them using UCAM"""
        with LocalMongoHook() as db:
            unresolved_serials = list(
                islice(db.find_deviceid_is_none(self.device_type), self.limit)
            )
            resolved_serials = {}

            with UcamHook() as ucam:
                for serial in unresolved_serials:
                    resolved = ucam.serial_to_id(serial)

                    if resolved:
                        resolved_serials[serial] = resolved

            records_updated = [
                db.update_many_serial_to_deviceid(serial, device_id, self.device_type)
                for serial, device_id in resolved_serials.items()
            ]

            # Logging
            logging.info(
                f"{len(unresolved_serials)} unresolved device_serials were collected"
                + f"from the DB (limit was {self.limit})"
            )
            logging.info(
                f"{len(resolved_serials)} serials were resolved into device_ids"
            )
            logging.info(
                f"{sum(records_updated)} records in the DB were updated with the resolved serials"
            )
        return True


class GroupRecordsOperator(BaseOperator):
    """
    Groups and update unprocessed records with their DMP ID. Cannot be limited to avoid data gaps

    The DMP expects DEVICEID-PATIENTID-STARTWEAR-ENDWEAR. Records are uploaded on a day basis.
    Given the 'day_cut_off' parameter, recordings are assigned to _today_ -> _tomorrow_ if their
    start-time is between day_cut_off_time _today_ and the same time _tomorrow_. If the recording
    started before the 'day_cut_off' then it belongs to _yesterday_ -> _today_.

    SPECIAL CASE: if set on 00:00:00, all recordings of _today_ belong to _today_ -> _today_

    Parameters
    ----------
    device_type: DeviceType
        the type of device to focus this operation on
    cut_off_time : str | "00:00:00"
        the time of day to 'cut off' recordings and assign them to today/tomorrow/yesterday
    """

    def __init__(
        self, device_type: DeviceType, cut_off_time: str = "00:00:00", **kwargs: Any
    ) -> None:
        """Construct Operator"""
        try:
            check_cut_off_time = datetime.strptime(cut_off_time, "%H:%M:%S").time()
        except ValueError as e:
            logging.error("{cut_off_time} not formatted like HH:MM:SS", exc_info=e)
            raise

        super().__init__(**kwargs)
        self.device_type = device_type
        self.cut_off_time = check_cut_off_time
        self.midnight = cut_off_time == "00:00:00"

    def execute(self, context: Any) -> bool:
        """Collect serials, and resolve them using UCAM"""
        with LocalMongoHook() as db:
            ungrouped_records = db.find_dmpid_is_none(self.device_type)
            grouped_records, groups = 0, set()

            for r in ungrouped_records:
                if self.midnight:
                    start = end = r.start.strftime("%Y%m%d")
                else:
                    # data belongs to this day -1 and 0, or 0 and +1
                    shift = -1 if r.start.time() < self.cut_off_time else 0
                    start = r.start + (shift * timedelta(days=1))
                    end = start + timedelta(days=1)

                dmp_id = (
                    f"{r.device_id.replace('-','')}-{r.patient_id.replace('-','')}-"
                    f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
                )
                db.custom_update_one(r._id, {"dmp_id": dmp_id})

                grouped_records += 1
                groups.add(dmp_id)

            logging.info(f"Grouping {len(ungrouped_records)} records")
            logging.info(f"{len(groups)} dmp_id unique groups were established")
            logging.info(f"{grouped_records} records updated on the DB")

        return True
