FROM apache/airflow:2.2.0-python3.8

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt
