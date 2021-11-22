from unittest.mock import patch

import click.testing
import pytest

import cli


@pytest.fixture
def runner() -> click.testing.CliRunner:
    """Return a CLI console for testing"""
    return click.testing.CliRunner()


def test_version_succeeds(runner: click.testing.CliRunner) -> None:
    """Test if the CLI initialises"""
    result = runner.invoke(cli.version)

    assert result.exit_code == 0


def test_bump_run(runner: click.testing.CliRunner) -> None:
    """Test if the bump version command runs"""
    bump_type = "patch"

    with patch("cli.run_command") as mock_run_command:

        result = runner.invoke(cli.bump, ["-b", bump_type])

        mock_run_command.assert_any_call(f"poetry version {bump_type}", True)
        assert result.exit_code == 0
