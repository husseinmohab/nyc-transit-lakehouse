# Image for the producer, the load generator and the Spark Structured
# Streaming jobs. One image for all three because they share the same
# `nyc_transit` package and the same Iceberg jars; three images would mean
# three places to keep the dependency set in sync.
FROM python:3.12-slim-bookworm

# Spark needs a JRE. Headless: no X11, which is ~200 MB of image for nothing.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/project

# Requirements first, so a source change does not invalidate the dependency
# layer. This is the single biggest factor in rebuild time.
COPY requirements/ requirements/
RUN pip install --no-cache-dir -r requirements/base.txt -r requirements/spark.txt

COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir --no-deps -e .

# Pre-fetch the Iceberg / Kafka / S3A jars into the image so the first
# container start does not spend a minute in Maven. Failure is tolerated:
# the jars resolve at runtime anyway, and a transient Maven outage must not
# break the image build.
RUN python -c "\
from nyc_transit.config import LakehouseSettings; \
from nyc_transit.streaming.session import iceberg_packages; \
print(','.join(iceberg_packages(LakehouseSettings(), with_kafka=True, with_s3=True)))" \
    > /opt/project/.spark-packages \
    && cat /opt/project/.spark-packages

# Non-root. The streaming job writes only to its warehouse (S3/MinIO) and to
# SPARK_LOCAL_DIRS, so it needs no write access to the image.
RUN useradd --create-home --shell /bin/bash spark \
    && mkdir -p /tmp/spark /home/spark/.ivy2 \
    && chown -R spark:spark /tmp/spark /home/spark /opt/project
USER spark

ENV HOME=/home/spark

CMD ["gtfs-producer", "--dry-run"]
