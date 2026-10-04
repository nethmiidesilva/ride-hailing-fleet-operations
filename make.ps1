<#
.SYNOPSIS
  Windows shim for the Makefile — GNU make is not installed by default on Windows.

.DESCRIPTION
  Exposes the same target names as the Makefile so the README can give one set of instructions.
  On Linux/macOS/WSL use `make <target>`; on Windows PowerShell use `.\make.ps1 <target>`.

.EXAMPLE
  .\make.ps1 up
  .\make.ps1 test-unit
  .\make.ps1 sql -Arg "select * from v_fleet_now"
  .\make.ps1 trigger-dag -Arg 2026-09-01
#>
param(
  [Parameter(Position = 0)]
  [string]$Target = "help",
  [Parameter(Position = 1)]
  [string]$Arg = ""
)

$ErrorActionPreference = "Stop"
$Compose  = "docker compose"
$Evidence = "docs/evidence"

function Invoke-Compose { param([string]$CommandLine) Invoke-Expression "$Compose $CommandLine" }

function Show-Help {
  Write-Host "fleet-lambda - available targets (PowerShell shim for the Makefile)" -ForegroundColor Cyan
  @(
    @("env",              "create .env from .env.example"),
    @("build",            "build every custom image"),
    @("up",               "start the whole stack and wait for health"),
    @("wait",             "block until every healthcheck is healthy"),
    @("down",             "stop the stack, keep volumes"),
    @("reset",            "stop the stack AND delete all volumes"),
    @("ps",               "container status and health"),
    @("logs",             "tail JSON logs from every service"),
    @("topics",           "describe the Kafka topics"),
    @("consume",          "print 20 live telemetry messages with partition and key"),
    @("psql",             "interactive psql session"),
    @("sql -Arg '<sql>'", "run one query"),
    @("trigger-dag -Arg <date>", "trigger the reconciliation DAG for a simulated date"),
    @("backfill -Arg <date>",    "replay a past day twice (idempotency demo)"),
    @("test-unit",        "unit tests (no stack needed)"),
    @("test-integration", "integration tests (stack must be up)"),
    @("test-e2e",         "end-to-end tests"),
    @("test-nfr",         "throughput / latency / resource measurements"),
    @("test-chaos",       "failure-injection scenarios"),
    @("test-all",         "every suite"),
    @("lint",             "ruff + black --check"),
    @("fmt",              "black + ruff --fix"),
    @("evidence",         "capture API/SQL/metrics evidence"),
    @("e2e-check",        "one-shot pipeline verification"),
    @("diagrams",         "render docs/diagrams/*.dot to PNG"),
    @("charts",           "regenerate report charts from real data"),
    @("report",           "rebuild the test-report tables from JUnit XML"),
    @("demo",             "scripted demonstration sequence"),
    @("open",             "print every UI URL"),
    @("clean-evidence",   "delete captured evidence")
  ) | ForEach-Object { "{0,-28} {1}" -f $_[0], $_[1] }
}

switch ($Target) {
  "help" { Show-Help }

  "env" {
    if (-not (Test-Path ".env")) { Copy-Item ".env.example" ".env"; Write-Host "created .env" }
    else { Write-Host ".env already exists" }
  }

  "build" { Invoke-Compose "build" }

  "up" {
    Invoke-Compose "up -d"
    & bash scripts/wait_for_services.sh
  }

  "wait"  { & bash scripts/wait_for_services.sh }
  "down"  { Invoke-Compose "down" }
  "reset" { Invoke-Compose "down -v --remove-orphans"; Write-Host "all volumes removed" }
  "ps"    { Invoke-Compose "ps" }
  "logs"  { Invoke-Compose "logs -f --tail=100" }

  "topics" {
    $env:MSYS_NO_PATHCONV = "1"
    Invoke-Compose "exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe"
  }

  "consume" {
    $env:MSYS_NO_PATHCONV = "1"
    Invoke-Compose ("exec kafka /opt/kafka/bin/kafka-console-consumer.sh " +
      "--bootstrap-server localhost:9092 --topic fleet.telemetry " +
      "--property print.key=true --property print.partition=true --max-messages 20 --timeout-ms 60000")
  }

  "psql" { Invoke-Compose "exec postgres psql -U fleet -d fleet" }

  "sql" {
    if (-not $Arg) { throw "usage: .\make.ps1 sql -Arg '<sql statement>'" }
    Invoke-Compose "exec -T postgres psql -U fleet -d fleet -c `"$Arg`""
  }

  "trigger-dag" {
    if (-not $Arg) { throw "usage: .\make.ps1 trigger-dag -Arg 2026-09-01" }
    Invoke-Compose "exec airflow-scheduler airflow dags trigger daily_reconciliation --conf '{\`"date\`": \`"$Arg\`"}'"
  }

  "backfill" { & bash scripts/chaos/replay_backfill.sh $Arg }

  "test-unit" {
    Invoke-Compose ("run --rm --no-deps tests pytest tests/unit -m unit -v " +
      "--junitxml=$Evidence/tests/junit-unit.xml " +
      "--html=$Evidence/tests/report-unit.html --self-contained-html " +
      "--cov=common --cov=streaming --cov=batch --cov=producers --cov=api " +
      "--cov-report=term-missing --cov-report=html:$Evidence/tests/coverage-html " +
      "--cov-report=xml:$Evidence/tests/coverage.xml")
  }

  "test-integration" {
    Invoke-Compose ("run --rm tests pytest tests/integration -m integration -v " +
      "--junitxml=$Evidence/tests/junit-integration.xml " +
      "--html=$Evidence/tests/report-integration.html --self-contained-html")
  }

  "test-e2e" {
    Invoke-Compose ("run --rm tests pytest tests/e2e -m e2e -v " +
      "--junitxml=$Evidence/tests/junit-e2e.xml " +
      "--html=$Evidence/tests/report-e2e.html --self-contained-html")
  }

  "test-nfr" {
    Invoke-Compose ("run --rm tests pytest tests/e2e -m nfr -v " +
      "--junitxml=$Evidence/tests/junit-nfr.xml " +
      "--html=$Evidence/tests/report-nfr.html --self-contained-html")
  }

  "test-chaos" { & bash scripts/chaos/run_all.sh }

  "test-all" {
    foreach ($t in @("test-unit", "test-integration", "test-e2e", "test-nfr", "test-chaos")) {
      Write-Host "`n=== $t ===" -ForegroundColor Cyan
      & $PSCommandPath $t
    }
  }

  "lint" { Invoke-Compose "run --rm --no-deps tests bash -c `"ruff check . && black --check .`"" }
  "fmt"  { Invoke-Compose "run --rm --no-deps tests bash -c `"black . && ruff check --fix .`"" }

  "evidence"  { Invoke-Compose "run --rm tests python scripts/collect_evidence.py" }
  "e2e-check" { Invoke-Compose "run --rm tests python scripts/e2e_check.py" }
  "charts"    { Invoke-Compose "run --rm tests python scripts/build_charts.py" }
  "report"    { Invoke-Compose "run --rm --no-deps tests python scripts/build_test_report.py" }

  "diagrams" {
    Invoke-Compose ("run --rm --no-deps tests bash -c " +
      "'for f in docs/diagrams/*.dot; do dot -Tpng `"`$f`" -o `"`${f%.dot}.png`"; echo rendered `${f%.dot}.png; done'")
  }

  "demo" { & bash scripts/demo.sh }

  "open" {
    Write-Host "FastAPI docs    http://localhost:8000/docs"
    Write-Host "Grafana         http://localhost:3000  (admin/admin)"
    Write-Host "Prometheus      http://localhost:9090"
    Write-Host "Airflow         http://localhost:8080  (admin/admin)"
    Write-Host "Spark UI        http://localhost:4040"
  }

  "clean-evidence" {
    foreach ($d in @("api", "sql", "tests", "scenarios", "metrics")) {
      $path = Join-Path $Evidence $d
      if (Test-Path $path) { Get-ChildItem $path -Recurse -File | Remove-Item -Force }
    }
    Write-Host "evidence cleared"
  }

  default { Write-Host "unknown target '$Target'`n" -ForegroundColor Red; Show-Help; exit 1 }
}
