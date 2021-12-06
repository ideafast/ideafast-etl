def test_dreem_dag_loaded(dagbag) -> None:
    """Test if the DAGs are loaded (i.e., no parse errors)"""
    result = dagbag.dags["dreem"]

    assert dagbag.import_errors == {}
    assert result is not None
    assert len(result.tasks) == 7
