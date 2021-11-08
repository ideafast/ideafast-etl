import logging
from datetime import datetime
from itertools import islice
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_utils import db
from etl_utils.db import DeviceType, Record
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

    def _download_metadata(limit: Optional[int] = None) -> None:
        """
        Retrieve records on Dreem's servers, filters only the new

        Parameters
        ----------
        limit : None | int
            limit of how many records to handle this run - useful for testing
            or managing workload in batches
        """
        with DreemHook(conn_id="dreem_kiel") as api:
            # get all results
            result = [r for r in api.get_metadata()]
            # create hashes of found records, deduct with known records
            known = db.get_hashes(DeviceType.DRM)

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
            new_ids = db.create_many_records(new_records) if new_records else []

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
        unresolved_dreem_uids = list(islice(db.get_unresolved_dreem_uids(), limit))
        resolved_dreem_uids = {}

        with UcamHook() as api:
            for uid in unresolved_dreem_uids:
                resolved = api.dreem_uid_to_serial(uid)

                if resolved:
                    resolved_dreem_uids[uid] = resolved

        records_updated = [
            db.update_many_drm_serials(uid, serial)
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
        unresolved_dreem_serials = list(
            islice(db.get_unresolved_device_serials(DeviceType.DRM), limit)
        )
        resolved_dreem_serials = {}

        with UcamHook() as api:
            for serial in unresolved_dreem_serials:
                resolved = api.serial_to_id(serial)

                if resolved:
                    resolved_dreem_serials[serial] = resolved

        records_updated = [
            db.update_many_device_ids(serial, device_id, DeviceType.DRM)
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
        unresolved_dreem_patients = list(
            islice(db.get_unresolved_patient_records(DeviceType.DRM), limit)
        )
        resolved_dreem_patients = 0

        with UcamHook() as api:
            for patient in unresolved_dreem_patients:
                resolved = api.resolve_patient_id(
                    patient.device_id, patient.start, patient.end
                )

                if resolved and db.update_record(
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

    def _group_records(limit: Optional[int] = None) -> None:
        """
        Group unprocessed records by day following DMP upload standards

        Parameters
        ----------
        limit : None | int
            limit of how many down and upload pairs to handle this run - useful for testing
            or managing workload in batches
        """
        logging.info(f"Grouping records with limit {limit}")

    def _extract_prep_load(
        run_id: str,
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
        logging.info(f"downloading and uploading data for {run_id} with limit {limit}")

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

    def _cleanup(run_id: str) -> None:
        logging.info(f"cleaning up for {run_id}")

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
        # uses a task pool to limit local storage
        pool="down_upload_pool",
        python_callable=_extract_prep_load,
        op_kwargs={"limit": 1, "run_id": "{{ run_id }}"},
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup,
        # trigger cleanup even if upstream tasks are skipped or failed
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={"run_id": "{{ run_id }}"},
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
