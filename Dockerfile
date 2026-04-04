FROM apache/airflow:2.11.0-python3.11

# Switch to root to install system-wide packages
USER root

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy your project files
COPY pyproject.toml uv.lock ./

# Install dependencies using uv into the system python
RUN uv pip install --system -r pyproject.toml

# Switch back to the airflow user
USER airflow

# Copy your DAGs (once you write them)
COPY dags/ /opt/airflow/dags/
#COPY --chown=airflow:0 dags/ /opt/airflow/dags/