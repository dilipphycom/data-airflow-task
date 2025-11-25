FROM apache/airflow:3.1.3-python3.12

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir \
    fastavro \
    google-cloud-storage \
    google-cloud-bigquery \
    pandas \
    pyarrow \
    requests

USER root
RUN apt-get update && apt-get install -y curl npm

# Install Dataform CLI globally
RUN npm install -g @dataform/cli

USER airflow
