# IDEA-FAST WP3 ETL Pipeline

A Extract, Transform, Load (ETL) pipeline based on [Apache Airflow](https://airflow.apache.org/). It periodically _extracts_ sensor data from wearables and mobile apps used in the [IDEA-FAST](www.idea-fast.eu) clinical observation study, _transforms_ the data by associating the appropriate anonymised participants, and _loads_ the data into the IDEA-FAST Data Management Portal.

## Local development

[Poetry](https://python-poetry.org/) is used for dependency management during development and [pyenv](https://github.com/pyenv/pyenv) to manage python installations, so please install both on your local machine. We use python 3.8 by default, so please make sure this is installed via pyenv, e.g.

```shell
pyenv install 3.8.0 && pyenv global 3.8.0
```

Once done, you can install dependencies for this project via:

```shell
poetry install
poetry run pre-commit install
```

When adding depencies, consider if these are for development or needed in production, then run with or without the `--dev` flag:
```shell
poetry add new-dependency
poetry add new-dependency --dev
```

Then, initiate a virtual environment to use those dependencies, running:

```shell
poetry shell
```

> Note that, for example, `apache-airflow` is a development dependency that is used for linting and type checking. Make sure you select the interpreter in your IDE that is identical to the `venv` you are working in, e.g.: ![](images/IDE.png)

## Running locally

Apache Airflow is ran using `Docker`, so please ensure you have a Docker client and Docker Compose (v1.29.1 or newer) installed on your local machine - see the [Docker website](https://docs.docker.com/get-started/). When on MacOS, the default memory for Docker is often not enough to run Apache Airflow smoothly. Adjust the allocated memory (from default 2.0 GB) to at least 4.0 in the Docker application > Preferences > Resources > Advanced > Memory (see also [here](https://docs.docker.com/desktop/mac/#advanced)).

### Environmental Variables
Remove `.example` from the `.env.example` filename, and adjust the values appropriately.

### Init
The very first time running Airflow, you will need to set up the database migrations and create accounts, run:

```shell
docker-compose up airflow-init
```

### Run

Once it's finished, you can spin up the Airflow containers, run:

```shell
docker-compose up
```

> If the image can't be found, check if you have the correct Airflow Image version in the `.env`, or build locally by running `poetry run build`.

You can check the status of the Docker containers by running `docker ps`, which should indiciate _(healthy)_ after a short while.

Navigate to _localhost:8080_ to see the Airflow UI. You can also check Airflow's status with some CLI commands, such as:

```shell
curl -X GET --user "$username:$userpass" "http://localhost:8080/api/v1/dags"
```

### Develop
Airflow will automatically pick up new 'DAGS' from the _/ideafast_etl/_ folder - it might take a short while (~a minute) for it to show up or have adjusted to the changes.

## Building the Docker Image

When building an image, we want to include the latest dependencies, this is handled in the command:

```shell
poetry run build
```

## Semantic versioning

Following common practice, the image and repo is updated following a _major.minor.patch_ versioning. Once changes are applied, please bump the version number using

```shell
poetry run bump -b {patch | minor | major}
```