FROM apache/airflow:2.7.0
RUN pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt