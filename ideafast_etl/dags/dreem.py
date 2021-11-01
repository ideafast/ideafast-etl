from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
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
            result = api.get_metadata()
            print(result)

    # Set all tasks
    download_latest_dreem_metadata = PythonOperator(
        task_id="download_latest_dreem_metadata",
        python_callable=_download_latest_dreem_metadata,
        default_args={"owner": "airflow"},
    )
    resolve_device_ids = DummyOperator(task_id="resolve_device_ids")
    resolve_patient_ids = DummyOperator(task_id="resolve_patient_ids")
    extract_prep_load = DummyOperator(
        task_id="extract_prep_load",
        # uses a task pool to limit local storage
        pool="down_upload_pool",
    )

    # Set dependencies between the static tasks

    (
        download_latest_dreem_metadata
        >> resolve_device_ids
        >> resolve_patient_ids
        >> extract_prep_load
    )
