# ==============================================================================================
# fleet-lambda — task runner.
#
# Every command a marker or examiner needs is a single `make` target.  On Windows without GNU
# make, use the equivalent PowerShell shim: `.\make.ps1 <target>` (same target names).
# ==============================================================================================
SHELL := /bin/bash
COMPOSE := docker compose
PROJECT := fleet-lambda
EVIDENCE := docs/evidence

.DEFAULT_GOAL := help
.PHONY: help env build up down reset ps logs logs-% wait demo topics psql sql api-docs \
        test-unit test-integration test-e2e test-chaos test-nfr test-all lint fmt \
        evidence report diagrams charts clean-evidence stop-producer start-producer \
        trigger-dag backfill open

## help: list every target with its description
help:
	@echo "fleet-lambda — available targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | sort

## env: create .env from .env.example (never overwrites an existing .env)
env:
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")
	@echo ".env ready"

## build: build every custom image (producers, streaming, airflow, api, tests)
build: env
	$(COMPOSE) build

## up: start the whole stack in the background and wait for health
up: env
	$(COMPOSE) up -d
	@$(MAKE) wait

## wait: block until every healthcheck reports healthy (used by CI and the README quick start)
wait:
	@bash scripts/wait_for_services.sh

## down: stop the stack, keep the volumes (data survives)
down:
	$(COMPOSE) down

## reset: stop the stack AND delete all volumes — a genuinely clean slate
reset:
	$(COMPOSE) down -v --remove-orphans
	@echo "all volumes removed; next 'make up' re-initialises Postgres, Kafka and the lake"

## ps: show container status and health
ps:
	$(COMPOSE) ps

## logs: tail structured JSON logs from every service
logs:
	$(COMPOSE) logs -f --tail=100

## logs-<service>: tail one service, e.g. make logs-stream-job
logs-%:
	$(COMPOSE) logs -f --tail=200 $*

## topics: describe the Kafka topics (partition layout evidence for P2)
topics:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

## consume: print 20 live telemetry messages with their partition and key
consume:
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server localhost:9092 --topic $${KAFKA_TOPIC:-fleet.telemetry} \
	  --property print.key=true --property print.partition=true \
	  --max-messages 20 --timeout-ms 60000

## psql: open an interactive psql session on the fleet database
psql:
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-fleet} -d $${POSTGRES_DB:-fleet}

## sql: run one query, e.g. make sql Q="select * from v_fleet_now"
sql:
	@$(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER:-fleet} -d $${POSTGRES_DB:-fleet} -c "$(Q)"

## trigger-dag: trigger the reconciliation DAG for a date, e.g. make trigger-dag D=2026-09-01
trigger-dag:
	$(COMPOSE) exec airflow-scheduler airflow dags trigger daily_reconciliation \
	  --conf '{"date": "$(D)"}'

## backfill: re-run a past simulated day twice to demonstrate idempotent recomputation
backfill:
	bash scripts/chaos/replay_backfill.sh $(D)

## test-unit: pure-Python and local-Spark tests (no stack needed)
test-unit:
	$(COMPOSE) run --rm --no-deps tests pytest tests/unit -m unit -v \
	  --junitxml=$(EVIDENCE)/tests/junit-unit.xml \
	  --html=$(EVIDENCE)/tests/report-unit.html --self-contained-html \
	  --cov=common --cov=streaming --cov=batch --cov=producers --cov=api \
	  --cov-report=term-missing --cov-report=html:$(EVIDENCE)/tests/coverage-html \
	  --cov-report=xml:$(EVIDENCE)/tests/coverage.xml

## test-integration: tests that require the running stack
test-integration:
	$(COMPOSE) run --rm tests pytest tests/integration -m integration -v \
	  --junitxml=$(EVIDENCE)/tests/junit-integration.xml \
	  --html=$(EVIDENCE)/tests/report-integration.html --self-contained-html

## test-e2e: full end-to-end flow across every service
test-e2e:
	$(COMPOSE) run --rm tests pytest tests/e2e -m e2e -v \
	  --junitxml=$(EVIDENCE)/tests/junit-e2e.xml \
	  --html=$(EVIDENCE)/tests/report-e2e.html --self-contained-html

## test-chaos: failure-injection scenarios (stops and restarts real containers)
test-chaos:
	bash scripts/chaos/run_all.sh

## test-nfr: throughput / latency / resource measurements
test-nfr:
	$(COMPOSE) run --rm tests pytest tests/e2e -m nfr -v \
	  --junitxml=$(EVIDENCE)/tests/junit-nfr.xml \
	  --html=$(EVIDENCE)/tests/report-nfr.html --self-contained-html

## test-all: unit + integration + e2e + chaos + nfr, writing all evidence
test-all: test-unit test-integration test-e2e test-nfr test-chaos
	@echo "all suites executed; evidence under $(EVIDENCE)/"

## lint: ruff check + black --check
lint:
	$(COMPOSE) run --rm --no-deps tests bash -c "ruff check . && black --check ."

## fmt: auto-format with black and fix what ruff can fix
fmt:
	$(COMPOSE) run --rm --no-deps tests bash -c "black . && ruff check --fix ."

## evidence: capture API responses, SQL outputs and metrics into docs/evidence/
evidence:
	$(COMPOSE) run --rm tests python scripts/collect_evidence.py

## e2e-check: one-shot verification that the whole pipeline is alive and producing data
e2e-check:
	$(COMPOSE) run --rm tests python scripts/e2e_check.py

## diagrams: render docs/diagrams/*.dot to PNG with Graphviz (inside the tests image)
diagrams:
	$(COMPOSE) run --rm --no-deps tests bash -c 'for f in docs/diagrams/*.dot; do dot -Tpng "$$f" -o "$${f%.dot}.png"; echo "rendered $${f%.dot}.png"; done'

## charts: regenerate the report charts from real data in Postgres
charts:
	$(COMPOSE) run --rm tests python scripts/build_charts.py

## report: rebuild TEST_REPORT.md from the JUnit XML and evidence files
report:
	$(COMPOSE) run --rm --no-deps tests python scripts/build_test_report.py

## demo: scripted 10-minute demonstration sequence (prints what to say and when)
demo:
	bash scripts/demo.sh

## stop-producer: chaos helper — kill ingestion so NoTelemetryReceived fires
stop-producer:
	$(COMPOSE) stop gps-producer

## start-producer: chaos helper — restore ingestion
start-producer:
	$(COMPOSE) start gps-producer

## open: print every UI URL
open:
	@echo "FastAPI docs    http://localhost:$${API_PORT:-8000}/docs"
	@echo "Grafana         http://localhost:$${GRAFANA_PORT:-3000}  (admin/admin)"
	@echo "Prometheus      http://localhost:$${PROMETHEUS_PORT:-9090}"
	@echo "Airflow         http://localhost:$${AIRFLOW_PORT:-8080}  (admin/admin)"
	@echo "Spark UI        http://localhost:4040"

## clean-evidence: delete captured evidence (forces a genuine re-run before reporting)
clean-evidence:
	rm -rf $(EVIDENCE)/api/* $(EVIDENCE)/sql/* $(EVIDENCE)/tests/* $(EVIDENCE)/scenarios/*
	@echo "evidence cleared"
