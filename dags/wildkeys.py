import logging
import shutil
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Dict, Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from ideafast_etl.hooks.db import DeviceType, LocalMongoHook, Record
from ideafast_etl.hooks.dmp import DmpHook
from ideafast_etl.hooks.wks import WildkeysHook
from ideafast_etl.operators.ucam import GroupRecordsOperator

# DAG setup with tasks
with DAG(
    dag_id="wildkeys",
    description="A complete WildKeys ETL pipeline",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    dag_run_download_folder = "/opt/airflow/downloads/{{dag.dag_id}}_{{ts_nodash}}"

    def _download_metadata(limit: Optional[int] = None) -> None:
        """
        Retrieve records on Wildkeys's servers. Patient information is provided.

        Parameters
        ----------
        limit : None | int
            limit of how many records to handle this run - useful for testing
            or managing workload in batches
        """
        with LocalMongoHook() as db:

            # get all results
            with WildkeysHook() as wks:
                participants = wks.get_participants()
                records_per_participant: Dict[str, dict] = {
                    p: wks.get_metadata(p) for p in participants
                }
                # records =  {partic1 : {date1 : [times], date2 : [times], ...}, partic2 : ...}

            # create hashes of found records, deduct with known records
            known = db.find_wildkeys_hashes()

            # generator for new records and records to update (consider 'new')
            new_record_gen = (
                Record(
                    _id=known[hash][1] if hash in known else None,
                    hash=hash,
                    manufacturer_ref=date,
                    device_type=DeviceType.WKS,
                    start=datetime.fromtimestamp(min(timestamps)),
                    end=datetime.fromtimestamp(max(timestamps)),
                    meta=dict(count=len(timestamps)),
                    patient_id=p,
                    device_id="WKS-CF6ZKY",
                )
                for p, records in records_per_participant.items()
                for date, timestamps in records.items()
                if (hash := Record.generate_hash(f"{p}/{date}", DeviceType.WKS))
                not in known
                or len(timestamps) != known[hash][0]
            )

            new_records = list(islice(new_record_gen, limit))
            new_ids = (
                [
                    db.custom_replace_one(r) if r._id else db.custom_insert_one(r)
                    for r in new_records
                ]
                if new_records
                else []
            )

            # Logging
            logging.info(
                f"{len([v for v in records_per_participant.values()])} WKS records found on the server"
            )
            logging.info(
                f"{len(new_ids)} new Wildkeys records added or updated in the DB (limit was {limit})"
            )

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
        dmp_mappings: dict = Variable.get("dmp_dataset_mappings", deserialize_json=True)

        with LocalMongoHook() as db, WildkeysHook() as wks, DmpHook() as dmp:
            unfinished_dmp_ids = list(
                islice(db.find_notuploaded_dmpids(DeviceType.WKS), limit)
            )
            finished_dmp_ids, processed_records = 0, 0

            for dmp_id in unfinished_dmp_ids:

                path = Path(download_folder) / dmp_id
                path.mkdir(parents=True, exist_ok=True)
                logging.debug(f"Created {str(path)} in local storage")

                records = db.find_records_by_dmpid(dmp_id)
                # dmp_dataset = records[0].dmp_dataset
                dmp_dataset: str = dmp_mappings.get("TEST")

                try:
                    # DOWNLOAD
                    total_records = len(records)
                    for index, record in enumerate(records, start=1):
                        wks.download_file(
                            record.patient_id, record.manufacturer_ref, path
                        )
                        logging.debug(
                            f"Downloaded {index}/{total_records} files for {dmp_id}"
                        )

                    # ZIP
                    zip_path = dmp.zip_folder(path)

                    # UPLOAD
                    # if any records already uploaded, perform DMP UPDATE
                    if any([r.is_uploaded for r in records]):
                        raise NotImplementedError
                    else:
                        # success = dmp.dmp_upload(dmp_dataset, zip_path)
                        # FIXME: currently 'is_uploaded' is set to false
                        # FIXME: change according to DMP versioning uploading system
                        success = dmp.upload(dmp_dataset, zip_path)

                    if not success:
                        # abort updating, retry next time
                        # Log something here...
                        continue

                    # UPDATE DB
                    update_count = db.update_dmpid_records_uploaded(dmp_id)
                    processed_records += update_count
                    finished_dmp_ids += 1

                except Exception as e:
                    logging.error(
                        f"Error in processing records with DMP ID {dmp_id}", exc_info=e
                    )

                finally:
                    # always remove local data, regardless of the outcome
                    dmp.rm_local_data(path.parent / f"{path.name}.zip")
                    logging.info("Removed (intermediate) downloaded files")
                    pass

            logging.info(
                f"retrieved {len(unfinished_dmp_ids)} folders to be uploaded with limit {limit}"
            )
            logging.info(f"uploaded {finished_dmp_ids} .zips to the DMP")
            logging.info(f"finished {processed_records} records on the DB")

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
        if Path(download_folder).is_dir():
            shutil.rmtree(download_folder)
        logging.info(f"Removed {download_folder} from local storage")

    # Set all tasks
    download_metadata = PythonOperator(
        task_id="download_metadata",
        python_callable=_download_metadata,
        op_kwargs={"limit": 15},
    )

    group_records = GroupRecordsOperator(
        task_id="group_records",
        cut_off_time="12:00:00",
        # TODO: discuss threshold with WP4 at TFTI
        device_type=DeviceType.WKS,
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

    download_metadata >> group_records >> extract_prep_load >> cleanup
