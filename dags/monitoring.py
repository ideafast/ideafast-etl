"""DAGs to regularly audit the sensor data ETL process"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ideafast_etl.hooks.db import LocalMongoHook
from ideafast_etl.hooks.db_monitoring import (
    LocalMonitorMongoHook,
    MonitoringRecord,
    DeviceMonitorRecord,
)

with DAG(
    dag_id="monitoring_weekly_digest",
    description="Pipeline health monitor weekly email digest",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2021, month=1, day=1),
    schedule_interval="0 7 * * 1",  # weekly on monday seven in the morning
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    def _send_report() -> None:
        """Send email digest to user accounts in the Airflow System"""
        pass

    # Set all tasks
    send_report = PythonOperator(
        task_id="send_report",
        python_callable=_send_report,
    )


def get_stats_query() -> dict:
    """Generate a reusable condition to check Record process"""

    def sub_query(got_to_step: str, but_not_step: str) -> dict:
        return {
            "$addToSet": {  # stores a set of UNIQUE result
                "$cond": [  # but only if...
                    {
                        "$and": [
                            {
                                "$ne": [f"${got_to_step}", None]
                            },  # the got_to_step is true
                            {
                                "$not": [f"${but_not_step}"]
                            },  # and the but_not_step is FALSY
                        ]
                    },
                    f"${got_to_step}",  # then, add the known culprit
                    "$$REMOVE",  # if not, do not add it
                ]
            }
        }

    return {
        "$group": {
            "_id": "$device_type",  # group by device type
            "total_db_records": {"$sum": 1},  # count all occurances
            "not_uploaded": {
                "$sum": {"$cond": [{"$eq": ["$is_uploaded", False]}, 1, 0]}
            },
            # A manufacturer_ref, but no device serial
            # Only a Dreem issue
            # "no_device_serial": process_query(
            #     "manufacturer_ref", "device_serial"
            # ),
            # A device serial, but no device ID
            "list_no_device_id": sub_query("device_serial", "device_id"),
            # A device ID, but no patient ID
            "list_no_patient_id": sub_query("device_id", "patient_id"),
            # A patient ID, but no DMP dataset assigned
            "list_no_dmp_dataset": sub_query("patient_id", "dmp_dataset"),
            # All ready, but somehow not uploaded. Possibly mismatch with DMP
            "list_not_uploaded": sub_query("dmp_id", "is_uploaded"),
        }
    }


with DAG(
    dag_id="monitoring_daily",
    description="Pipeline health monitor daily calculation",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2021, month=1, day=1),
    schedule_interval="0 6 * * *",  # daily at six in the morning
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    def _generate_report() -> None:
        """Audit sensor data DB and Pipeline Health and store report"""
        with LocalMongoHook() as db:
            stats = db.query_stats([get_stats_query()])

            report = MonitoringRecord(
                date=datetime.now(),
                pipeline_stats={
                    stat["_id"]: DeviceMonitorRecord(
                        **{k: v for k, v in stat.items() if k != "_id"}
                    )
                    for stat in stats
                },
            )

            with LocalMonitorMongoHook() as monitoring_db:
                monitoring_db.custom_insert_one(report)
                print(monitoring_db.get_latest_stats())

    # Set all tasks
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )
