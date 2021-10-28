FROM apache/airflow:2.2.0

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY /init/pools.json /opt/airflow/init/pools.json