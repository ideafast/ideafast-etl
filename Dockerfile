FROM apache/airflow:2.2.0

COPY requirements.txt /tmp/

RUN pip install -r /tmp/requirements.txt