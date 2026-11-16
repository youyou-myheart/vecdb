#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/dispatch-acceptance.status
trap 'printf "failed\n" > reports/dispatch-acceptance.status' ERR
INTEGRATION=1 bash scripts/go.sh test -race -count=1 ./... > reports/dispatch-all-tests.log 2>&1
bash scripts/go.sh vet ./... > reports/dispatch-vet.log 2>&1
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -race -count=1 -timeout=10m -run '^(TestClusteredOneProcessPipeline|TestRetentionOneProcessPipeline)$' -v ./cmd/vector-server > reports/dispatch-onebox.log 2>&1
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server > reports/dispatch-build.log 2>&1
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl >> reports/dispatch-build.log 2>&1
PYTHONPATH=sdk/python .venv/bin/python -m unittest discover -s sdk/python/tests -v > reports/dispatch-python-tests.log 2>&1
.venv/bin/python scripts/verify_dispatch.py > reports/dispatch-processes.log 2>&1
.venv/bin/python scripts/verify_retention.py > reports/dispatch-retention-regression.log 2>&1
printf 'passed\n' > reports/dispatch-acceptance.status
