import importlib
import sys
from typing import ContextManager

import pytest


class localise_utils:
    def __enter__(self) -> None:
        sys.modules["etl_utils"] = importlib.import_module("ideafast_etl.etl_utils")

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del sys.modules["etl_utils"]


@pytest.fixture
def utils_module() -> ContextManager[None]:
    """Use the Airflow DAG localised module within the test suite (argubaly a hack)"""
    with localise_utils():
        yield
