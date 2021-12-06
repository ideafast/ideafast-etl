from unittest.mock import patch

import pytest


@pytest.fixture
def mock_ucam_op():
    """Return a mocked UcamHook imported via ideafast_etl.operators"""
    with patch("ideafast_etl.operators.ucam.UcamHook") as ucam_hook_mock:
        yield ucam_hook_mock


@pytest.fixture
def mock_mongo_op():
    """Return a mocked MongoHook imported via ideafast_etl.operators"""
    with patch("ideafast_etl.operators.ucam.LocalMongoHook") as mongo_hook_mock:
        yield mongo_hook_mock
