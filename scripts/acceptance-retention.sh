#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/retention-acceptance.status
trap 'printf "failed\n" > reports/retention-acceptance.status' ERR
INTEGRATION=1 bash scripts/go.sh test -race -count=1 ./... > reports/retention-all-tests.log 2>&1
bash scripts/go.sh vet ./... > reports/retention-vet.log 2>&1
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -race -timeout=10m -run '^TestRetentionOneProcessPipeline$' -v ./cmd/vector-server > reports/retention-onebox-pipeline.log 2>&1
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server > reports/retention-build.log 2>&1
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl >> reports/retention-build.log 2>&1
PYTHONPATH=sdk/python .venv/bin/python -m unittest discover -s sdk/python/tests -v > reports/retention-python-tests.log 2>&1
.venv/bin/python scripts/verify_retention.py > reports/retention-processes.log 2>&1
printf 'passed\n' > reports/retention-acceptance.status
