from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from etl_utils.db import DeviceType, Record, get_hashes
from etl_utils.hooks.drm import DreemJwtHook

# DAG setup with tasks
with DAG(
    dag_id="dreem",
    description="A complete Dreem ETL pipeline",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
) as dag:

    def _download_latest_dreem_metadata() -> None:
        with DreemJwtHook(conn_id="dreem_kiel") as api:
            # get all results
            result = [r for r in api.get_metadata()]

            # create hashes of found records, deduct with known records
            all_records = {
                Record.generate_hash(r["id"], DeviceType.DRM) for r in result
            }
            new_records = all_records - get_hashes(DeviceType.DRM)
            print(new_records)

    # Set all tasks
    download_latest_dreem_metadata = PythonOperator(
        task_id="download_latest_dreem_metadata",
        python_callable=_download_latest_dreem_metadata,
    )
    resolve_device_ids = DummyOperator(task_id="resolve_device_ids")
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
