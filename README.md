# IDEA-FAST WP3 ETL Pipeline

A Extract, Transform, Load (ETL) pipeline based on [Apache Airflow](https://airflow.apache.org/). It periodically _extracts_ sensor data from wearables and mobile apps used in the [IDEA-FAST](www.idea-fast.eu) clinical observation study, _transforms_ the data by associating the appropriate anonymised participants, and _loads_ the data into the IDEA-FAST Data Management Portal.

## Development

[Poetry](https://python-poetry.org/) is used for dependency management and
[pyenv](https://github.com/pyenv/pyenv) to manage python installations, so
please install both on your local machine. We use python 3.8 by default, so
please make sure this is installed via pyenv, e.g.

    pyenv install 3.8.0 && pyenv global 3.8.0

Once done, you can install dependencies for this project via:

    poetry install
    poetry run pre-commit install

In order to run Apache Airflow within the virtual environment, spin up a `poetry shell` whilst parsing the `dev.env` environmental variables into it. Then, initiate an airflow instance:

    env $(cat dev.env) poetry shell
    airflow standalone

> **NOTE**: the Airflow `standalone` is not to be used in production, only for getting started and local development.

> **NOTE 2**: Airflow explicitely states that it does not support `Poetry` as we do here, primarily as it does not incorporates the referred `constraints.txt` that is used when installing through `pip`. For now, no issues seem to occur - do let us know if this is different on you machine.

Navigate to _localhost:8080_ to see the Airflow UI. If you need to interact within the shell in the terminal, be sure to open another tab and join the shel with the `.env` included as above. For example, you can see the available dags by running:

    airflow dags list