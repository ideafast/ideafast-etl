import click.testing
import pytest

from ideafast_etl.utils import cli


@pytest.fixture
def runner():
    return click.testing.CliRunner()


def test_main_succeeds(runner):
    result = runner.invoke(cli.version)

    assert result.exit_code == 0
