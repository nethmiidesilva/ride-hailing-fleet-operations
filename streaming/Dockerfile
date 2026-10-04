# ==============================================================================================
# Spark Structured Streaming image (speed layer).
#
# Built on python:3.11-slim rather than the official `spark` image because:
#   * PySpark from pip already contains the full Spark distribution, so the official image adds
#     nothing except a 1 GB base and a different Python version to manage;
#   * this way the streaming job, the batch job and the tests run on one identical interpreter.
#
# The Kafka and PostgreSQL connector jars are baked in at build time so no Maven resolution
# happens when the container starts — start-up is deterministic and works offline.
#
# Build context is the PROJECT ROOT (see docker-compose.yml).
# ==============================================================================================
FROM python:3.11-slim-bookworm

# Spark 3.5 is built and tested against Java 17.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements-common.txt ./
COPY streaming/requirements.txt ./streaming/
RUN pip install --no-cache-dir -r streaming/requirements.txt

ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG POSTGRES_JDBC_VERSION=42.7.4
ENV SPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN set -eux; \
    base=https://repo1.maven.org/maven2; \
    curl -fsSL -o "$SPARK_JARS/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "$base/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$SPARK_JARS/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "$base/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "$SPARK_JARS/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "$base/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSL -o "$SPARK_JARS/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "$base/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fsSL -o "$SPARK_JARS/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "$base/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

COPY common/ ./common/
COPY streaming/ ./streaming/
COPY batch/ ./batch/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# The job owns /data (lake, checkpoints) so it must be able to write there.
RUN useradd --create-home --uid 10002 spark \
 && mkdir -p /data/lake /data/checkpoints /shared \
 && chown -R spark:spark /app /data /shared
USER spark

CMD ["python", "-m", "streaming.stream_job"]
