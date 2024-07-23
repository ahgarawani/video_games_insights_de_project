FROM apache/airflow:2.9.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY setup_connections.py $AIRFLOW_HOME

RUN mkdir /home/airflow/.kaggle && echo '{"username":"ahgarawani","key":"5bc0c70e56417c6bd837950e5acb02a1"}' > /home/airflow/.kaggle/kaggle.json
