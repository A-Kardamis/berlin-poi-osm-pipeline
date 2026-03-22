FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y gdal-bin libgdal-dev && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
