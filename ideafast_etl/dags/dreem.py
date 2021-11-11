import logging
from datetime import datetime, timedelta
from itertools import islice
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_utils.hooks.db import DeviceType, LocalMongoHook, Record
from etl_utils.hooks.drm import DreemHook
from etl_utils.hooks.ucam import UcamHook

# DAG setup with tasks
with DAG(
    dag_id="dreem",
    description="A complete Dreem ETL pipeline",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    dag_run_download_folder = "{{dag.dag_id}}_{{ts_nodash}}"

    def _download_metadata(limit: Optional[int] = None) -> None:
        """
        Retrieve records on Dreem's servers, filters only the new

        Parameters
        ----------
        limit : None | int
            limit of how many records to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:

            # get all results
            with DreemHook(conn_id="dreem_kiel") as drm:
                result = [r for r in drm.get_metadata()]

            # create hashes of found records, deduct with known records
            known = db.find_hashes(DeviceType.DRM)

            # generator for new unknown records
            new_record_gen = (
                Record(
                    _id=None,
                    hash=hash,
                    manufacturer_ref=r["id"],
                    device_type=DeviceType.DRM,
                    start=datetime.fromtimestamp(r["report"]["start_time"]),
                    end=datetime.fromtimestamp(r["report"]["stop_time"]),
                    meta=dict(dreem_uid=r["device"]),
                )
                for r in result
                if (hash := Record.generate_hash(r["id"], DeviceType.DRM)) not in known
            )

            new_records = list(islice(new_record_gen, limit))
            new_ids = db.custom_insert_many(new_records) if new_records else []

            # Logging
            logging.info(f"{len(result)} Dreem records found on the server")
            logging.info(
                f"{len(new_ids)} new Dreem records added to the DB (limit was {limit})"
            )

    def _resolve_device_serials(limit: Optional[int] = None) -> None:
        """
        Retrieve records with unknown device serials, and try to resolve them

        Note
        ----
        Requires agreement with Dreem how to map Dreem uid to serials

        Parameters
        ----------
        limit : None | int
            limit of how many device uids to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:
            unresolved_dreem_uids = list(
                islice(db.find_drm_deviceserial_is_none(), limit)
            )
            resolved_dreem_uids = {}

            with UcamHook() as api:
                for uid in unresolved_dreem_uids:
                    resolved = api.dreem_uid_to_serial(uid)

                    if resolved:
                        resolved_dreem_uids[uid] = resolved

            records_updated = [
                db.update_drmuid_to_serial(uid, serial)
                for uid, serial in resolved_dreem_uids.items()
            ]

            # Logging
            logging.info(
                f"{len(unresolved_dreem_uids)} unresolved device uids were collected"
                + f"from the DB (limit was {limit})"
            )
            logging.info(f"{len(resolved_dreem_uids)} uids were resolved into serials")
            logging.info(
                f"{sum(records_updated)} records in the DB were updated with the resolved serials"
            )

    def _resolve_device_ids(limit: Optional[int] = None) -> None:
        """
        Retrieve records with unknown device IDs, and try to resolve them

        Note
        ----
        Requires update once UCAM includes device serials

        Parameters
        ----------
        limit : None | int
            limit of how many device serials to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:
            unresolved_dreem_serials = list(
                islice(db.find_deviceid_is_none(DeviceType.DRM), limit)
            )
            resolved_dreem_serials = {}

            with UcamHook() as ucam:
                for serial in unresolved_dreem_serials:
                    resolved = ucam.serial_to_id(serial)

                    if resolved:
                        resolved_dreem_serials[serial] = resolved

            records_updated = [
                db.update_many_serial_to_deviceid(serial, device_id, DeviceType.DRM)
                for serial, device_id in resolved_dreem_serials.items()
            ]

            # Logging
            logging.info(
                f"{len(unresolved_dreem_serials)} unresolved device_serials were collected"
                + f"from the DB (limit was {limit})"
            )
            logging.info(
                f"{len(resolved_dreem_serials)} serials were resolved into device_ids"
            )
            logging.info(
                f"{sum(records_updated)} records in the DB were updated with the resolved serials"
            )

    def _resolve_patient_ids(limit: Optional[int] = None) -> None:
        """
        Retrieve records with unknown patient IDs, and try to resolve them

        Parameters
        ----------
        limit : None | int
            limit of how many device serials to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:
            unresolved_dreem_patients = list(
                islice(db.find_patientid_is_none(DeviceType.DRM), limit)
            )
            resolved_dreem_patients = 0

            with UcamHook() as ucam:
                for patient in unresolved_dreem_patients:
                    resolved = ucam.resolve_patient_id(
                        patient.device_id, patient.start, patient.end
                    )

                    if resolved and db.custom_update_one(
                        patient._id, dict(patient_id=resolved)
                    ):
                        resolved_dreem_patients += 1

            # Logging
            logging.info(
                f"{len(unresolved_dreem_patients)} unresolved device_serials were collected"
                + f" from the DB (limit was {limit})"
            )
            logging.info(
                f"{resolved_dreem_patients} patient_ids were resolved and updated on the DB"
            )

    def _group_records() -> None:
        """
        Group and update unprocessed records with their DMP ID. Cannot be limited to avoid data gaps

        The DMP expects DEVICEID-PATIENTID-STARTWEAR-ENDWEAR. Records are uploaded on a day basis.
        For Dreem, this is determined based on the start-time of the recording:
            start-time between 12:00:00 Monday and 11:59:59 Tuesday is data for `Monday-Tuesday`
            start-time 12:00:00 or later on Tuesday is data for `Tuesday-Monday`

        Note
        ----
        Above threshold needs to be discussed with WP4 at TFTI
        """
        with LocalMongoHook() as db:
            ungrouped_records = db.find_dmpid_is_none(DeviceType.DRM)
            grouped_records = 0
            groups = set()

            midday = datetime.strptime("12:00:00", "%H:%M:%S").time()

            for r in ungrouped_records:
                # start-time before 12:00 then data belong to yesterday+today
                if r.start.time() < midday:
                    end = r.start.strftime("%Y%m%d")
                    start = (r.start - timedelta(days=1)).strftime("%Y%m%d")
                # start-time after 12:00 then data belong to today+tomorrow
                else:
                    start = r.start.strftime("%Y%m%d")
                    end = (r.start + timedelta(days=1)).strftime("%Y%m%d")

                dmp_id = f"{r.device_id.replace('-','')}-{r.patient_id.replace('-','')}-{start}-{end}"
                db.custom_update_one(r._id, {"dmp_id": dmp_id})

                grouped_records += 1
                groups.add(dmp_id)

            logging.info(f"Grouping {len(ungrouped_records)} records")
            logging.info(f"{len(groups)} dmp_id unique groups were established")
            logging.info(f"{grouped_records} records updated on the DB")

    def _extract_prep_load(
        download_folder: str,
        limit: Optional[int] = None,
    ) -> None:
        """
        Download, zip, and upload records to the DMP

        Unfortunately, dynamically generating task within a DAG is proven to be difficult and hacky.
        For now, this task just sequentially handles down and upload.

        Parameters
        ----------
        limit : None | int
            limit of how many down and upload pairs to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:
            unfinished_dmp_ids = list(
                islice(db.find_notuploaded_dmpids(DeviceType.DRM), limit)
            )

            # with DreemHook(conn_id="dreem_kiel") as dreem_api, DmpHook() as dmp_api:
            #     for dmp_id in unfinished_dmp_ids:
            #         # if we find a grouping (all records with the same dmp_id) has both
            #         # uploaded and not-uploaded records, the pipeline needs to UPDATE
            #         # the DMP. Else, if all not uploaded, just UPLOAD

            #         # get all records with this dmp_id
            #         pass

            logging.info(
                f"downloading and uploading {len(unfinished_dmp_ids)} folders with limit {limit}"
            )

            # - sort
            # - groupby device
            # - group by participant
            # - group by day
            # create folder
            # download into that folder
            # zip
            # upload
            # update record
            # remove folder
            # if fail, still remove folder

    def _cleanup(download_folder: str) -> None:
        """
        Clean up any downloads into the

        Unfortunately, dynamically generating task within a DAG is proven to be difficult and hacky.
        For now, this task just sequentially handles down and upload.

        Parameters
        ----------
        limit : None | int
            limit of how many down and upload pairs to handle this run - useful for testing
            or managing workload in batches
        """
        logging.info(f"cleaning up for {download_folder}")

    # Set all tasks
    download_metadata = PythonOperator(
        task_id="download_metadata",
        python_callable=_download_metadata,
        op_kwargs={"limit": 1},
    )

    resolve_device_serials = PythonOperator(
        task_id="resolve_device_serials",
        python_callable=_resolve_device_serials,
        op_kwargs={"limit": 1},
    )

    resolve_device_ids = PythonOperator(
        task_id="resolve_device_ids",
        python_callable=_resolve_device_ids,
        op_kwargs={"limit": 1},
    )

    resolve_patient_ids = PythonOperator(
        task_id="resolve_patient_ids",
        python_callable=_resolve_patient_ids,
        op_kwargs={"limit": 1},
    )

    group_records = PythonOperator(
        task_id="group_records",
        python_callable=_group_records,
        op_kwargs={"limit": 1},
    )

    extract_prep_load = PythonOperator(
        task_id="extract_prep_load",
        python_callable=_extract_prep_load,
        op_kwargs={"download_folder": dag_run_download_folder, "limit": 1},
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        # trigger cleanup even if upstream tasks are skipped or failed
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={"download_folder": dag_run_download_folder},
    )

    # Set linear dependencies between the static tasks

    (
        download_metadata
        >> resolve_device_serials
        >> resolve_device_ids
        >> resolve_patient_ids
        >> group_records
        >> extract_prep_load
        >> cleanup
    )
