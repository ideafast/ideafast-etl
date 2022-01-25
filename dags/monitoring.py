"""DAGs to regularly audit the sensor data ETL process"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

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
        pass

    # Set all tasks
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
    )
