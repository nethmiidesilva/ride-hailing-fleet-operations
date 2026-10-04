# ==============================================================================================
# Airflow image (orchestration of the batch layer).
#
# The stock Airflow image has no JVM, so `spark-submit` would fail.  This image adds:
#   * OpenJDK 17  — the JVM Spark 3.5 targets
#   * pyspark 3.5.3 — which also provides the `spark-submit` launcher on PATH
#   * the PostgreSQL JDBC driver — the batch job reads daily_expenses over JDBC
#   * psycopg v3 — used by the DAG's own validation/load/quality tasks
#
# Airflow's constraint file is deliberately NOT applied to these extra packages: pyspark is not
# an Airflow provider and pinning it here keeps the streaming and batch layers on one version.
# ==============================================================================================
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

ARG SPARK_VERSION=3.5.3
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN pip install --no-cache-dir \
        pyspark==${SPARK_VERSION} \
        "psycopg[binary]==3.2.3" \
        prometheus-client==0.21.1

# Bake the JDBC driver into the Spark jar directory so `spark-submit` needs no --packages.
ENV SPARK_JARS=/home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
RUN curl -fsSL -o "$SPARK_JARS/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

# `common/` and `batch/` are bind-mounted here by docker compose; PYTHONPATH makes both the DAG
# file and the spark-submit'ed job able to `import common...`.
ENV PYTHONPATH=/opt/airflow/project \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/airflow
