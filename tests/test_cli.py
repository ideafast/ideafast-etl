from unittest.mock import patch

import click.testing
import pytest

import cli


@pytest.fixture
def runner() -> click.testing.CliRunner:
    return click.testing.CliRunner()


def test_version_succeeds(runner: click.testing.CliRunner) -> None:

    result = runner.invoke(cli.version)

    assert result.exit_code == 0


def test_abc_aborted(runner: click.testing.CliRunner) -> None:
    bump_type = "patch"

    with patch("cli.run_command") as mock_run_command:

        result = runner.invoke(cli.bump, ["-b", bump_type])

        mock_run_command.assert_any_call(f"poetry version {bump_type}", True)
        assert result.exit_code == 0
