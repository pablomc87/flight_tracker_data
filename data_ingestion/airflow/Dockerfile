FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.1}
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION:-2.10.1}" -r /requirements.txt