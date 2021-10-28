from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

# DAG setup with tasks
with DAG(
    dag_id="dreem",
    description="A complete Dreem ETL pipeline",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Set all tasks
    download_latest_dreem_metadata = DummyOperator(
        task_id="download_latest_dreem_metadata"
    )
    resolve_device_ids = DummyOperator(task_id="resolve_device_ids")
    resolve_patient_ids = DummyOperator(task_id="resolve_patient_ids")
    extract_prep_load = DummyOperator(
        task_id="extract_prep_load", pool="down_upload_pool"
    )

    # Set dependencies between the static tasks

    (
        download_latest_dreem_metadata
        >> resolve_device_ids
        >> resolve_patient_ids
        >> extract_prep_load
    )
