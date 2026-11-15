#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/onebox-pipeline.status
trap 'printf "failed\n" > reports/onebox-pipeline.status' ERR
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -race -timeout=10m \
  -run '^TestOneProcessPipeline$' -v ./cmd/vector-server > reports/onebox-pipeline.log 2>&1
printf 'passed\n' > reports/onebox-pipeline.status
cat reports/onebox-pipeline.json
