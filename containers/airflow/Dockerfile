FROM apache/airflow:2.9.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY setup_connections.py $AIRFLOW_HOME
