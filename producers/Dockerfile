# ==============================================================================================
# Image for both ingestion sources (gps_producer and expense_producer).
#
# One image, two entrypoints: the two producers share the simulated clock, the config module and
# the metrics definitions, so building them separately would duplicate ~150 MB for no benefit.
# The compose file picks the module with `command:`.
#
# Build context is the PROJECT ROOT (see docker-compose.yml) so that `common/` can be copied in.
# ==============================================================================================
FROM python:3.11-slim-bookworm

# curl is used by the container healthcheck; librdkafka comes from the confluent-kafka wheel.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-common.txt ./
COPY producers/requirements.txt ./producers/
RUN pip install --no-cache-dir -r producers/requirements.txt

COPY common/ ./common/
COPY producers/ ./producers/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Run as a non-root user: the container writes only to /data and /shared, both of which are
# volumes chowned below.
RUN useradd --create-home --uid 10001 fleet \
 && mkdir -p /data /shared \
 && chown -R fleet:fleet /app /data /shared
USER fleet

CMD ["python", "-m", "producers.gps_producer"]
