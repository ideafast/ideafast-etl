# IDEA-FAST WP3 ETL Pipeline

A Extract, Transform, Load (ETL) pipeline based on [Apache Airflow](https://airflow.apache.org/). It periodically _extracts_ sensor data from wearables and mobile apps used in the [IDEA-FAST](www.idea-fast.eu) clinical observation study, _transforms_ the data by associating the appropriate anonymised participants, and _loads_ the data into the IDEA-FAST Data Management Portal.

## Development

[Poetry](https://python-poetry.org/) is used for dependency management during development and [pyenv](https://github.com/pyenv/pyenv) to manage python installations, so please install both on your local machine. We use python 3.8 by default, so please make sure this is installed via pyenv, e.g.

```shell
pyenv install 3.8.0 && pyenv global 3.8.0
```

Once done, you can install dependencies for this project via:

```shell
poetry install
poetry run pre-commit install
```

## Running locally

Apache Airflow is ran using `Docker`, so please ensure you have a Docker client and Docker Compose (v1.29.1 or newer) installed on your local machine - see the [Docker website](https://docs.docker.com/get-started/). When on MacOS, the default memory for Docker is often not enough to run Apache Airflow smoothly. Adjust the allocated memory (from default 2.0 GB) to at least 4.0 in the Docker application > Preferences > Resources > Advanced > Memory (see also [here](https://docs.docker.com/desktop/mac/#advanced)). You can check how much memory is currently allocated via the terminal if preffered, by running:

```shell
docker run --rm "debian:buster-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

The very first time running Airflow, you will need to set up the database migrations and create accounts, run:

```shell
docker-compose up airflow-init
```

Once it's finished, you can spin up the Airflow containers, run:

```shell
docker-compose up
```

You can check the status of the Docker containers by running `docker ps`, which should indiciate _(healthy)_ after a short while.
Navigate to _localhost:8080_ to see the Airflow UI. You can also check Airflow's status with some CLI commands, such as:

```shell
airflow dags list
docker-compose run airflow-worker airflow info
```

## Building the Docker Image

To deploy the ETL Pipeline as a package, we will have to extend the Apache Airflow image to include any used dependencies and DAG files. To do so, we will export the dependencies as used in `Poetry`, then build the image using the Dockerfile:

```shell
# poetry export -f requirements.txt --output requirements.txt
```