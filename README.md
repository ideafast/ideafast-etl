[![Tests](https://github.com/ideafast/ideafast-etl/actions/workflows/tests.yml/badge.svg)](https://github.com/ideafast/ideafast-etl/actions/workflows/tests.yml)
[![Language grade: JavaScript](https://img.shields.io/lgtm/grade/python/g/ideafast/ideafast-etl.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/ideafast/ideafast-etl/context:python)

# IDEA-FAST WP3 ETL Pipeline

An Extract, Transform, Load (ETL) pipeline based on [Apache Airflow](https://airflow.apache.org/). It periodically _extracts_ sensor data from wearables and mobile apps used in the [IDEA-FAST](www.idea-fast.eu) clinical observation study, _transforms_ the data by associating the appropriate anonymised participants, and _loads_ the data into the IDEA-FAST Data Management Portal.

## Running locally

Apache Airflow is ran using `Docker`, so please ensure you have a Docker client and Docker Compose (v1.29.1 or newer) installed on your local machine - see the [Docker website](https://docs.docker.com/get-started/).

> When on MacOS, the default memory for Docker is often not enough to run Apache Airflow smoothly. Adjust the allocated memory (from default 2.0 GB) to at least 4.0 in the Docker application > Preferences > Resources > Advanced > Memory (see also [here](https://docs.docker.com/desktop/mac/#advanced)).


### Initialise
1. Remove `.example` from the `.env.example` filename, and adjust the values appropriately.
1. The very first time running Airflow, you will need to set up the database migrations and create accounts, run:

    ```shell
    docker-compose up airflow-init
    ```

1. Once it's finished, you can spin up the Airflow containers, run:

    ```shell
    docker-compose up -d
    ```
    You can check the status of the Docker containers by running `docker ps`, which should indiciate _(healthy)_ after a short while.
1. Navigate to _localhost:8080_ to see the Airflow UI. You can also check Airflow's status with some CLI commands, such as:
    ```shell
    curl -X GET --user "$username:$userpass" "http://localhost:8080/api/v1/dags"
    ```

1. It is recommended to properly shut down the docker container once you are finished. Run
    ```shell
    docker-compose down
    ```
    to do so. Note that volumes are persisted - including set variables, connections and user accounts.

## Local development

[Poetry](https://python-poetry.org/) is used for dependency management during development and [pyenv](https://github.com/pyenv/pyenv) to manage python installations, so please install both on your local machine. We use python 3.8 by default, so please make sure this is installed via pyenv, e.g.

```shell
pyenv install 3.8.0 && pyenv global 3.8.0
```

Once done, clone the repo to your machine and install dependencies for this project via:

```shell
poetry install
poetry run pre-commit install
```

> Note that `click` is a core dependency solely for using the CLI locally, but is actually not required for the Docker image deployment.

When adding depencies in development, consider if these are for development or needed in production, then run with or without the `--dev` flag:
```shell
poetry add new-dependency
poetry add new-dependency --dev
```

Then, initiate a virtual environment to use those dependencies, running:

```shell
poetry shell
```

> Note that, for example, `apache-airflow` is a development dependency that is used for linting and type checking. Make sure you select the interpreter in your IDE that is identical to the `venv` you are working in.

### Develop
Airflow will automatically pick up new 'DAGS' from the _/ideafast_etl/_ folder - it might take a short while (~a minute) for it to show up or have adjusted to the changes.

If you make substantial changes, consider bumping the repo's version, build a new Docker image and pushing it to hub.docker.com:

```shell
poetry run bump -b patch  # or minor, or major
poetry run build
poetry run publish
```

To check the current version of the Poetry package, local Git (_Git and Poetry are synced when using above `bump` command_) and Docker image (_only adopts the Poetry/Git version when actually built_), run:
```shell
poetry run version
```

### CLI with Airflow Docker

You can interact with Airflow in the docker containers through the command line using `docker compose run`. On MacOS / Linux, use the provided wrapper script to simplify the command (run `chmod +x airflow.sh` once to enable it) - see also the [docs](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#running-the-cli-commands).

Ensure that you are in a `poetry shell` to use the `airflow` dev dependency, and that you spun up the docker containers using `docker-compose up`. Then prepend any CLI command with
```shell
./airflow.sh [command]                               # MacOs/Linux
docker-compose run airflow-worker airflow [command]  # Windows
```

Example cli commands (using the wrapper script):
```shell
./airflow.sh info   # get generic info
./airflow.sh dags list   # list all known dags
./airflow.sh dags test dummy_dag 2021-10-26   # run 'dummy_dag' DAG once (whether it is paused or not)
/airflow.sh tasks test dummy_dag join_datasets 2021-10-26 # run 'join_datasets' task from the 'dummy_dag' DAG once (whether it is paused or not)
./airflow.sh dags test dummy_dag 2021-10-26 --dry-run   # dry-run 'dummy_dag' DAG to see {{ templates variables }} rendered

./airflow.sh cheat-sheet   # show CLI commands
```

### Maintainability

Below are some design choices based on our need to maintain the pipeline in the future with the least amount of work.

#### Variables

For any known [variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html), [connection URIs](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) (e.g., username/passwords), Airflow enables us to pass these through as environmental variables (e.g., the mongo DB connection string). However, any environmental variable is not shown (or editable) in the UI. In order to allow the Airflow maintainer to easily update any connection URI or variable when needed, connections and variables that have the potential to change need to be added manually in the UI. _As long as you don't delete the `postrgres-db-volume` volume, these details will persists across spin ups and downs_. Any internal connections or variables are added through environmental variables.

> Note, DAGS also have access to Airflow Engine variables at runtime, which can be used through {{Jinja templating}}. See a [list of out-of-the-box available variables](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html).

#### (sub)modules

This repository is somewhat odd as a 'package', as only the `/ideafast_etl` subfolder gets mounted in the docker container. As such, module import works differently from within Apache Airflow as it does for the testing suite. To 'quickly' overcome this for development, the test-suite needs to manually add the reference to the (for Airflow) local import using the [`conftest.py'](/tests/conftest.py) fixture. Airflow documentation suggest using [Plugins](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) but for our purposes we mainly want share-able function across DAGS - and will use the current setup until a better approach is found.

### Running Tests, Type Checking, Linting and Code Formatting

[Nox](https://nox.thea.codes/) is used for automation and standardisation of tests, type hints, automatic code formatting, and linting. Any contribution needs to pass these tests before creating a Pull Request.

To run all these libraries:

    poetry run nox -r

Or individual checks by choosing one of the options from the list:

    poetry run nox -rs [tests, mypy, lint, black]

--------
--------
--------

# Note about _atomicity_ and _idempotency_

As outlined by [Bas Harenslak and Julian Rutger de Ruiter](https://github.com/BasPH/data-pipelines-with-apache-airflow), Apache Airflow is a very powerful tool - especially when needed for timewindow based batch operations. Ideally, DAGs and their tasks are **idempotent**, such that calling the same task multiple times with the same inputs should not change the overall output. In addition, tasks execution should be **atomic**, such that succeed and produce some proper result or fail in a manner that does not affect the state of the system (think of sending a 'success' email before a tasks has completed sucesfully).

Whilst we closely follow the best practice in atomicity, the IDEA-FAST pipeline steers away from idempotency. This is purposely done, as the extracted data is not finite across timewindows, nor is it 'ready' for extraction at an agreed time. As such, we cannot ensure the idempotency of rerunning a specific task - even if our Airflow DAGs are designed that way. Instead, IDEA-FAST uses the ETL pipeline to get extract and load any new data (somewhat) as soon as it's available - so that our clinicians 'on the ground' can inspect the data as soon as possible. In result, our ETL pipeline effectively polls the data from our device providers and acts when new data is found.

To support the data _as available_ approach (contrasting a fixed time window approach), the pipeline utilise an additional database to keep a historical record of past processed files - allowing detecting of new and old additions to process.
