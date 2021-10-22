from unittest.mock import patch

import click.testing
import pytest

from ideafast_etl.utils import cli


@pytest.fixture
def runner():
    return click.testing.CliRunner()


def test_version_succeeds(runner):

    result = runner.invoke(cli.version)

    assert result.exit_code == 0


@patch("ideafast_etl.utils.cli.run_command")
def test_abc_aborted(mock_run_command, runner):
    bump_type = "patch"

    result = runner.invoke(cli.bump, ["-b", bump_type])

    mock_run_command.assert_any_call(f"poetry version {bump_type}", True)
    assert result.exit_code == 0
