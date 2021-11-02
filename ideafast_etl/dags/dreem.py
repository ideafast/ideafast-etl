import logging
from datetime import datetime
from itertools import islice
from typing import Optional

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from etl_utils.db import DeviceType, DreemRecord, create_many_records, get_hashes
from etl_utils.hooks.drm import DreemJwtHook

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

    def _download_latest_dreem_metadata(limit: Optional[int] = None) -> None:
        """
        Retrieve records on Dreem's servers, filters only the new

        Parameters
        ----------
        limit : None | int
            limit of how many records to handle this run - useful for testing
            or managing workload in batches
        """
        with DreemJwtHook(conn_id="dreem_kiel") as api:
            # get all results
            result = [r for r in api.get_metadata()]
            # create hashes of found records, deduct with known records
            known = get_hashes(DeviceType.DRM)

            # generator for new unknown records
            new_record_gen = (
                DreemRecord(
                    _id=None,
                    hash=hash,
                    manufacturer_ref=r["id"],
                    device_serial=r["device"],
                    download_url=r["url"],
                    device_type=DeviceType.DRM,
                    start=datetime.fromtimestamp(r["report"]["start_time"]),
                    end=datetime.fromtimestamp(r["report"]["stop_time"]),
                )
                for r in result
                if (hash := DreemRecord.generate_hash(r["id"], DeviceType.DRM))
                not in known
            )

            new_records = list(islice(new_record_gen, limit))
            new_ids = create_many_records(new_records) if new_records else []

            # Logging
            logging.info(f"{len(result)} Dreem records found on the server")
            logging.info(
                f"{len(new_ids)} new Dreem records added to the DB (limit was {limit})"
            )

    def _resolve_device_ids(limit: Optional[int] = None) -> None:
        """
        Retrieve database records with unresolved device IDs, and try to resolve them

        Parameters
        ----------
        limit : None | int
            limit of how many records to handle this run - useful for testing
            or managing workload in batches
        """
        pass

    # Set all tasks
    download_latest_dreem_metadata = PythonOperator(
        task_id="download_latest_dreem_metadata",
        python_callable=_download_latest_dreem_metadata,
        op_kwargs={"limit": 1},
    )
    resolve_device_ids = PythonOperator(
        task_id="resolve_device_ids",
        python_callable=_resolve_device_ids,
        op_kwargs={"limit": 1},
    )
    resolve_patient_ids = DummyOperator(task_id="resolve_patient_ids")
    extract_prep_load = DummyOperator(
        task_id="extract_prep_load",
        # uses a task pool to limit local storage
        pool="down_upload_pool",
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
    )

    # Set dependencies between the static tasks

    (
        download_latest_dreem_metadata
        >> resolve_device_ids
        >> resolve_patient_ids
        >> extract_prep_load
    )
