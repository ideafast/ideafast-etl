from pathlib import Path

import pytest
from airflow.models import DagBag

PROJECT_DIR = Path(__file__).parent.parent


@pytest.fixture()
def dagbag() -> DagBag:
    """
    Load the dags from the Airflow folder, with local imports overridden in conftest.py

    The DagBag creation will check the integrity for all DAGS, and raise exceptions such
    as when a DAG has an invalid setup or cyclic reference between tasks or when duplicate
    dag_id's exists.
    """
    return DagBag(
        # Dag folder needs to be identical as used in docker-compose.yaml
        dag_folder=Path(PROJECT_DIR) / "dags",
        include_examples=False,
    )
