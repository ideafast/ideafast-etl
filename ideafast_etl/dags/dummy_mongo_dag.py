from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


# Python Operator Methods
def _add_mongo_value(doc: str) -> None:
    """use the Mongo Hook to add a variable to the db"""
    with MongoHook() as db:
        db.insert_one(mongo_collection="ideafast_etl", doc={"doc_id": doc})


def _print_mongo_values() -> None:
    """use the Mongo Hook to retreive variables from the db"""
    with MongoHook() as db:
        test_data = db.get_collection(mongo_collection="ideafast_etl")
        print(test_data)


def _delete_mongo_value(doc: str) -> None:
    """use the Mongo Hook to delete a variable from the db"""
    with MongoHook() as db:
        db.delete_one(mongo_collection="ideafast_etl", filter_doc={"doc_id": doc})


# DAG setup with tasks
with DAG(
    dag_id="dummy_mongo_dag_v6",
    description="Dummy dag to test Mongo Connection",
    # arbitrary start date, UNLESS USED TO RUN HISTORICALLY!
    start_date=datetime(year=2019, month=11, day=1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Set all tasks
    add_mongo_value = PythonOperator(
        task_id="add_mongo_value",
        python_callable=_add_mongo_value,
        op_args=["{{ run_id }}"],
    )
    print_mongo_values_2 = PythonOperator(
        task_id="print_mongo_values_2", python_callable=_print_mongo_values
    )
    delete_mongo_value = PythonOperator(
        task_id="delete_mongo_value",
        python_callable=_delete_mongo_value,
        op_args=["{{ run_id }}"],
    )
    print_mongo_values_3 = PythonOperator(
        task_id="print_mongo_values_3", python_callable=_print_mongo_values
    )

    # Set dependencies between the tasks
    (
        add_mongo_value
        >> print_mongo_values_2
        >> delete_mongo_value
        >> print_mongo_values_3
    )
