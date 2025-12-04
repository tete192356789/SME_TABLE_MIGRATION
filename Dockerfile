FROM apache/airflow:3.1.3

# Switch to root to install system packages if needed
USER root

# Install system dependencies (optional)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# ⭐ THIS IS WHERE PACKAGES ARE INSTALLED ⭐
RUN pip install --no-cache-dir --user -r /requirements.txt

# Copy config (optional, can also use volumes)
COPY --chown=airflow:root ./config /opt/airflow/config