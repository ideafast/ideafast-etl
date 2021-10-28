from pathlib import Path
from typing import ContextManager

import pytest
from airflow.models import DagBag

project_dir = Path(__file__).parent.parent


@pytest.fixture()
def dagbag(utils_module: ContextManager) -> DagBag:
    """Load the dags from the Airflow folder, with local imports overridden in conftest.py"""
    return DagBag(
        # Dag folder needs to be identical as used in docker-compose.yaml
        dag_folder=Path(project_dir) / "ideafast_etl/dags",
        include_examples=False,
    )


def test_example_mongo_dag_loaded(dagbag: DagBag) -> None:

    result = dagbag.dags["example_mongo_dag"]

    assert dagbag.import_errors == {}
    assert result is not None
    assert len(result.tasks) == 4
