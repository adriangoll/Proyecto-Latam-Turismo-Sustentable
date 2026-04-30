FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

USER airflow

# Copiamos requirements
COPY docker/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copiamos Solo lo necesario en vez de todo el proyecto
COPY pipelines/ /opt/airflow/pipelines/
COPY airflow/dags/ /opt/airflow/dags/
COPY dags/utils.py /opt/airflow/utils.py

# Para que Airflow encuentre pipelines y utils
ENV PYTHONPATH=/opt/airflow