def test_dummy_dag_loaded(dagbag) -> None:
    """Test if the DAGs are loaded (i.e., no parse errors)"""
    result = dagbag.dags["dummy_dag"]

    assert dagbag.import_errors == {}
    assert result is not None
    assert len(result.tasks) == 7
