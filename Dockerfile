# ──────────────────────────────────────────────
# Stage 1: Pull Spark binaries
# ──────────────────────────────────────────────
FROM apache/spark:3.4.2 AS spark

# ──────────────────────────────────────────────
# Stage 2: Airflow + Spark combined image
# ──────────────────────────────────────────────
FROM apache/airflow:3.1.5-python3.11

USER root

# Install Java (required for Spark) and system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        wget \
        curl \
        build-essential \
        gcc \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Copy Spark from the Spark stage
COPY --from=spark /opt/spark /opt/spark

# Copy local Spark JARs (delta, kafka connectors, etc.)
COPY spark/ivy/jars/*.jar /opt/spark/jars/

# Set Spark + Java env vars
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ──────────────────────────────────────────────
# Python dependencies (as airflow user)
# ──────────────────────────────────────────────
USER airflow

COPY requirements.txt /requirements.txt

# Step 1: Install all requirements EXCEPT pyspark under Airflow constraints
#         Use constraints-3.11.txt to match the pinned Python version
RUN pip install --no-cache-dir \
        -r /requirements.txt \
        --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.11.txt

# Step 2: Force-install the correct pyspark version to match Spark 3.4.2
#         (overrides whatever the constraints file pulled in)
RUN pip install --no-cache-dir --force-reinstall \
        pyspark==3.4.2 \
        delta-spark==2.4.0

USER airflow
