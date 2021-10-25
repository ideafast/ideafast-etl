import tempfile
from typing import Any

import nox
from nox_poetry import session

nox.options.sessions = "black", "lint", "mypy", "tests"
LOCATIONS = "ideafast_etl", "tests", "noxfile.py"


def install_with_constraints(session: nox.Session, *args: str, **kwargs: Any) -> None:
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@session(python=["3.8"])
def tests(session: nox.Session) -> None:
    """Run test suite"""
    args = session.posargs or ["--cov"]
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(session, "coverage[toml]", "pytest", "pytest-cov")
    session.run("pytest", *args)


@session(python=["3.8"])
def lint(session: nox.Session) -> None:
    """Provide lint warnings to help enforce style guide."""
    args = session.posargs or LOCATIONS
    install_with_constraints(
        session,
        "flake8",
        "flake8-aaa",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
    )
    session.run("flake8", *args)


@session(python=["3.8"])
def mypy(session: nox.Session) -> None:
    """Provide type warnings to help enfore style guide"""
    args = session.posargs or LOCATIONS
    install_with_constraints(session, "mypy")
    session.run("mypy", *args)


@session(python=["3.8"])
def black(session: nox.Session) -> None:
    """Automatic format code following black codestyle:
    https://github.com/psf/black.
    """
    args = session.posargs or LOCATIONS
    session.install("black")
    session.run("black", *args)
