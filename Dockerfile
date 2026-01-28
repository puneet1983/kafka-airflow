FROM apache/airflow:2.8.1-python3.10

USER airflow
RUN pip install --upgrade pip setuptools wheel \
    && pip install duckdb kafka-python pandas

# FROM apache/airflow:2.8.1

# Temporarily switch to root to install system tools
# USER root
# RUN apt-get update && apt-get install -y build-essential g++ python3-dev

# Copy requirements
# COPY requirements.txt .

# USER airflow
# RUN pip install duckdb-engine kafka-python pandas

