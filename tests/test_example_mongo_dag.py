from airflow.models import DagBag


def test_example_mongo_dag_loaded(dagbag: DagBag) -> None:

    result = dagbag.dags["example_mongo_dag"]

    assert dagbag.import_errors == {}
    assert result is not None
    assert len(result.tasks) == 4
