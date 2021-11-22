from airflow.models import DagBag


def test_example_mongo_dag_loaded(dagbag: DagBag) -> None:
    """Test if the DAGs are loaded (i.e., no parse errors)"""
    result = dagbag.dags["example_mongo_dag"]

    assert dagbag.import_errors == {}
    assert result is not None
    assert len(result.tasks) == 4
