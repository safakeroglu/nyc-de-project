FROM apache/airflow:2.6.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAG and script files
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY scripts/ ${AIRFLOW_HOME}/scripts/

# Copy credentials
COPY credentials/ ${AIRFLOW_HOME}/credentials/

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=${AIRFLOW_HOME}/credentials/nyc-de-project00-90f0a92d4ebb.json