from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_utils import db

# DAG setup with tasks
with DAG(
    dag_id="example_mongo_dag",
    description="Dummy dag to test Mongo Connection",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Set all tasks
    add_mongo_value = PythonOperator(
        task_id="add_mongo_value",
        python_callable=db.add_mongo_value,
        op_args=["{{ run_id }}"],
    )
    print_mongo_values_1 = PythonOperator(
        task_id="print_mongo_values_1",
        python_callable=db.print_mongo_values,
        op_args=["{{ run_id }}"],
    )
    delete_mongo_value = PythonOperator(
        task_id="delete_mongo_value",
        python_callable=db.delete_mongo_value,
        op_args=["{{ run_id }}"],
    )
    print_mongo_values_2 = PythonOperator(
        task_id="print_mongo_values_2",
        python_callable=db.print_mongo_values,
        op_args=["{{ run_id }}"],
    )

    # Set dependencies between the tasks
    (
        add_mongo_value
        >> print_mongo_values_1
        >> delete_mongo_value
        >> print_mongo_values_2
    )
