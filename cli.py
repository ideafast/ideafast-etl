import subprocess  # noqa
from typing import Optional

import click

DOCKER_REGISTRY = "ideafast/etl"
VERSION_BUMPS = ["patch", "minor", "major"]


def run_command(command: str, capture: bool = False) -> subprocess.CompletedProcess:
    """Helper method to run shell commands"""

    # NOTE: shell=True is highly discouraged due to potential shell-injection attacks. Use with care.
    return subprocess.run([command], shell=True, capture_output=capture)  # noqa


def docker_image_exists(version: str) -> str:
    """Check if docker image exists based on version."""
    command = f"docker images -q {DOCKER_REGISTRY}:{version}"
    res = run_command(command, True)
    # The hash ID of the image if it exists
    return res.stdout.decode("ascii").rstrip()


def get_version() -> str:
    """Return the current Poetry tag version (which is synced with Git and Docker)"""
    res = run_command("poetry version -s", True)
    return res.stdout.decode("ascii").rstrip()


def get_docker_tags() -> str:
    res = run_command(
        f"docker images {DOCKER_REGISTRY} --format '{{.Tag}} | sort | head -n 1'", True
    )
    return res.stdout.decode("ascii").rstrip()


def get_latest_git_tag() -> str:
    res = run_command("git tag --sort=-refname |head -n 10", True)
    return res.stdout.decode("ascii").rstrip()


def bump_version(bump: str) -> str:
    """Bump current Poetry tag version and update git"""
    poetry_res = run_command(f"poetry version {bump}", True)
    new_version = poetry_res.stdout.decode("ascii").rsplit(maxsplit=1)[1]
    run_command(f"git tag {new_version}")
    return new_version


def push_version_remote(version: str) -> None:
    """Push local tag to git remote origin."""
    run_command(f"git push origin {version}")


def build_docker(version: str) -> None:
    run_command(f"docker build -f Dockerfile -t {DOCKER_REGISTRY}:{version}")


@click.group()
def cli() -> None:
    """CLI for building and deploying the ETL pipeline in Docker."""


@cli.command()
def build(bump: Optional[str]) -> None:
    """Build the docker image and update git and poetry tags."""

    if bump not in VERSION_BUMPS:
        version = get_version()
        message = f"You are not bumping the version\n. \
        Are you sure you want to rebuild {DOCKER_REGISTRY} - {version}?"

        click.confirm(message, abort=True)

        # build the current version! txt.rsplit(maxsplit=1)

    else:
        new_version = bump_version(bump)
        click.echo(
            f"Poetry and lightweight git tag bumped to {new_version}\nBuilding Docker image now."
        )
        build_docker(new_version)
        click.confirm("Do you want to push this tag to GitHub too?", abort=True)
        push_version_remote(new_version)


@cli.command()
def version() -> None:
    """Displays current version tags"""
    click.echo(f"Poetry: {get_version()}")
    click.echo(f"Git: {get_latest_git_tag()}")
    click.echo(f"Docker: {get_latest_git_tag()}")


if __name__ == "__main__":
    cli()
