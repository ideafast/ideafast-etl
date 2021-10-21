import subprocess  # noqa
from typing import Optional

import click

DOCKER_REGISTRY = "ideafast/etl"
VERSION_BUMPS = ["patch", "minor", "major"]


def run_command(command: str, capture: bool = False) -> subprocess.CompletedProcess:
    """Helper method to run shell commands"""

    # NOTE: shell=True is highly discouraged due to potential shell-injection attacks. Use with care.
    return subprocess.run([command], shell=True, capture_output=capture)  # noqa


def get_version() -> Optional[str]:
    """Return the current Poetry tag version (which is synced with Git and Docker)"""
    res = run_command("poetry version -s", True)
    return res.stdout.decode("ascii").rstrip() or None


def get_latest_docker_tag() -> Optional[str]:
    res = run_command(
        f"docker images {DOCKER_REGISTRY} --format {{{{.Tag}}}} "
        f"| sort -r | grep -v 'latest' | head -n 1",
        True,
    )
    return res.stdout.decode("ascii").rstrip() or None


def get_latest_git_tag() -> Optional[str]:
    res = run_command("git tag --sort=-refname |head -n 1", True)
    return res.stdout.decode("ascii").rstrip() or None


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
    # Export the poetry (non-dev) requirements
    run_command(
        "poetry export -f requirements.txt --output requirements.txt --without-hashes"
    )
    # Build the image, which picks up the requirements file
    run_command(
        f"docker build -f Dockerfile -t {DOCKER_REGISTRY}:{version} -t {DOCKER_REGISTRY}:latest ."
    )


@click.group()
def cli() -> None:
    """CLI for building and deploying the ETL pipeline in Docker."""


@cli.command()
@click.option(
    "--bump_type",
    "-b",
    required=True,
    type=click.Choice(VERSION_BUMPS, case_sensitive=False),
)
def bump(bump_type: str) -> str:
    """Bump version number."""

    new_version = bump_version(bump_type)

    if click.confirm("Do you want to push this tag to GitHub too?"):
        push_version_remote(new_version)

    return new_version


@cli.command()
@click.option(
    "--bump_type", "-b", type=click.Choice(VERSION_BUMPS, case_sensitive=False)
)
def build(bump_type: Optional[str] = None) -> None:
    """Build the docker image and optionally bump version"""
    version = get_version()
    docker_version = get_latest_docker_tag()
    build_version = version  # if not bumped

    if bump_type in VERSION_BUMPS:
        build_version = bump(bump_type)

    # the current poetry/git version is higher or no Docker version exists
    if docker_version and int(version.replace(".", "")) <= int(
        docker_version.replace(".", "")
    ):
        message = (
            f"You are not bumping the version\n"
            f"Are you sure you want to rebuild {DOCKER_REGISTRY} - {version}?"
        )
        click.confirm(message, abort=True)

    build_docker(build_version)
    click.echo(
        f"\nCompleted. Latest Docker image: {build_version}\n"
        f"To use it, update the image version in your .env and run docker-compose up -d"
    )


@cli.command()
def publish() -> None:
    """Publish latest Docker image"""
    version = get_latest_docker_tag()
    if not version and click.confirm(
        f"No local image found for version {get_version()}\n"
        f"Do you want to build AND then publish it?",
        abort=True,
    ):
        build()

    run_command(f"docker push {DOCKER_REGISTRY} --all-tags")


@cli.command()
def version() -> None:
    """Displays current version tags"""
    click.echo(f"Poetry: {get_version()}")
    click.echo(f"Git: {get_latest_git_tag()}")
    click.echo(f"Docker: {get_latest_docker_tag()}")


if __name__ == "__main__":
    cli()
