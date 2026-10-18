# Airflow image: Airflow + its providers + this project's package + dbt.
#
# Built on the official Airflow image rather than installing Airflow with pip,
# because Airflow's dependency set only resolves against its published
# constraints file, and mixing that resolution with pyspark/dbt/great-expectations
# in one `pip install` is how you get an unbootable scheduler. Hence also why
# requirements/airflow.txt is kept separate from requirements/dev.txt.
FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow
WORKDIR /opt/project

COPY --chown=airflow:root requirements/ requirements/

# Providers resolved against Airflow's own constraints: this is the step that
# keeps the scheduler bootable.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir \
    -r requirements/airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# The rest without constraints: pyspark, dbt and great-expectations are not in
# Airflow's constraint file, and forcing them through it fails.
RUN pip install --no-cache-dir \
    -r requirements/spark.txt \
    -r requirements/dbt.txt \
    -r requirements/quality.txt

COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src/ src/
RUN pip install --no-cache-dir --no-deps -e .

ENV PYTHONPATH=/opt/project/src:/opt/project/airflow/plugins \
    DBT_PROFILES_DIR=/opt/project/dbt/nyc_transit
