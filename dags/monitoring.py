"""DAGs to regularly audit the sensor data ETL process"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ideafast_etl.hooks.db import LocalMongoHook

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

            # count: total data records, inc. of which not uploaded
            stats = db.query_stats(
                [
                    {
                        "$group": {
                            "_id": "$device_type",  # group by device type
                            "total": {"$sum": 1},  # count all occurances
                            "not_uploaded": {
                                "$sum": {
                                    "$cond": [{"$eq": ["$is_uploaded", False]}, 1, 0]
                                }
                            },
                            # # NO device serial (only applicable to dreem?)
                            # "no_device_serial": {
                            #     "$addToSet": {
                            #         "$cond": [
                            #             {"$eq": ["$device_serial", None]},
                            #             "$meta.dreem_uid",
                            #             "$$REMOVE",
                            #         ]
                            #     }
                            # },
                            # A device serial, but no device ID
                            "no_device_id": {
                                "$addToSet": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$eq": ["$device_id", None]},
                                                {"$ne": ["$device_serial", None]},
                                            ]
                                        },
                                        "$device_serial",
                                        "$$REMOVE",
                                    ]
                                }
                            },
                            # A device ID, but no patient ID
                            "no_patient_id": {
                                "$addToSet": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$eq": ["$patient_id", None]},
                                                {"$ne": ["$device_id", None]},
                                            ]
                                        },
                                        "$device_id",
                                        "$$REMOVE",
                                    ]
                                }
                            },
                            # A patient ID, but no DMP dataset assigned
                            "no_dmp_dataset": {
                                "$addToSet": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$eq": ["$dmp_dataset", None]},
                                                {"$ne": ["$patient_id", None]},
                                            ]
                                        },
                                        "$patient_id",
                                        "$$REMOVE",
                                    ]
                                }
                            },
                            # All ready, but somehow not uploaded. Possibly mismatch with DMP
                            "no_uploaded": {
                                "$addToSet": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$eq": ["$is_uploaded", False]},
                                                {"$ne": ["$dmp_id", None]},
                                            ]
                                        },
                                        "$dmp_id",
                                        "$$REMOVE",
                                    ]
                                }
                            },
                        }
                    }
                ]
            )

            # report = {d["_id"]:  for}
            for group in stats:
                print(group)

    # Set all tasks
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )
