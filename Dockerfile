FROM apache/spark:3.4.2 AS spark

FROM apache/airflow:3.1.5



USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        wget \
        curl \
        build-essential \
        gcc \
        procps \
    && rm -rf /var/lib/apt/lists/*




USER airflow

USER airflow
COPY requirements.txt /requirements.txt

RUN pip install \
    -r /requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.5/constraints-3.10.txt

RUN pip uninstall -y pyspark

# Install matching PySpark
RUN pip install pyspark==3.4.2 delta-spark==2.4.0

USER root
# Install Spark 3.4.2 (Scala 2.12)
# Copy Spark from Spark image
COPY spark/ivy/jars/*.jar /opt/spark/jars/

COPY --from=spark /opt/spark /opt/spark



ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
#ENV SPARK_HOME=/opt/spark
#ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow
