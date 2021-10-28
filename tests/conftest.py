import importlib
import sys
from pathlib import Path
from typing import Any, Generator

import pytest
from airflow.models import DagBag

PROJECT_DIR = Path(__file__).parent.parent


class localise_utils:
    def __enter__(self) -> None:
        sys.modules["etl_utils"] = importlib.import_module("ideafast_etl.etl_utils")

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        del sys.modules["etl_utils"]


@pytest.fixture()
def local_utils_module() -> Generator[None, None, None]:
    """Use the Airflow DAG localised module within the test suite (argubaly a hack)"""
    with localise_utils():
        yield


@pytest.fixture()
def dagbag(local_utils_module: Generator[None, None, None]) -> DagBag:
    """Load the dags from the Airflow folder, with local imports overridden in conftest.py"""
    return DagBag(
        # Dag folder needs to be identical as used in docker-compose.yaml
        dag_folder=Path(PROJECT_DIR) / "ideafast_etl/dags",
        include_examples=False,
    )
