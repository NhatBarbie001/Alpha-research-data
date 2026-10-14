FROM apache/airflow:3.1.0-python3.12

# Cài alphadata vào virtualenv riêng để không đụng dependency của Airflow.
# DAG gọi nó qua @task.external_python(python=$ALPHADATA_PYTHON).
RUN python -m venv /home/airflow/alphadata-venv
COPY --chown=airflow:root pyproject.toml /home/airflow/alphadata/
COPY --chown=airflow:root src /home/airflow/alphadata/src
RUN /home/airflow/alphadata-venv/bin/pip install --no-cache-dir "/home/airflow/alphadata[postgres]"

ENV ALPHADATA_PYTHON=/home/airflow/alphadata-venv/bin/python
