#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/graph-acceptance.status
trap 'printf "failed\n" > reports/graph-acceptance.status' ERR
bash scripts/go.sh test -race -count=1 ./... > reports/graph-all-unit.log 2>&1
INTEGRATION=1 bash scripts/go.sh test -race -count=1 ./internal/vector > reports/graph-tikv-integration.log 2>&1
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -race -timeout=10m -run '^TestGraphOneProcessPipeline$' -v ./cmd/vector-server > reports/graph-onebox-pipeline.log 2>&1
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server > reports/graph-build.log 2>&1
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl >> reports/graph-build.log 2>&1
python3 scripts/verify_graph_replication.py > reports/graph-processes.log 2>&1
.venv/bin/python scripts/verify_subscriptions.py > reports/subscriptions.log 2>&1
printf 'passed\n' > reports/graph-acceptance.status
