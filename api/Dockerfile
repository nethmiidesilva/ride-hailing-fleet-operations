# ==============================================================================================
# FastAPI serving-layer image.
#
# No JVM and no Spark here: the API only reads PostgreSQL (and does one Kafka metadata call for
# the health check), so it stays a ~200 MB image that starts in under a second.  Keeping the
# serving layer this light is a deliberate part of the architecture — it can be scaled
# horizontally without dragging Spark along.
#
# Build context is the PROJECT ROOT (see docker-compose.yml).
# ==============================================================================================
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-common.txt ./
COPY api/requirements.txt ./api/
RUN pip install --no-cache-dir -r api/requirements.txt

COPY common/ ./common/
COPY api/ ./api/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 10003 apiuser \
 && mkdir -p /data /shared \
 && chown -R apiuser:apiuser /app
USER apiuser

EXPOSE 8000

# One worker: the API is read-only and I/O bound against Postgres, and a single worker keeps the
# prometheus_client counters in one process so /metrics-prom is not split across workers.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", \
     "--no-access-log", "--proxy-headers"]
