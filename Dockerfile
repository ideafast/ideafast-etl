FROM apache/airflow:2.2.0-python3.8

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER ${AIRFLOW_UID}
COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt
