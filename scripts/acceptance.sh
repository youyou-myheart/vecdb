#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/acceptance.status
trap 'printf "failed\n" > reports/acceptance.status' ERR
bash scripts/go.sh test -count=1 -race -coverprofile=reports/unit.cover ./... > reports/unit.log 2>&1
bash scripts/go.sh vet ./... > reports/vet.log 2>&1
bash scripts/go.sh tool cover -func=reports/unit.cover > reports/coverage.txt
ANN_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -run TestHNSWRecallAcceptance -v ./internal/vector > reports/acceptance-ann.log 2>&1
INTEGRATION=1 bash scripts/go.sh test -count=1 -race -v ./... > reports/acceptance-go.log 2>&1
bash scripts/acceptance-onebox.sh > reports/onebox-pipeline-summary.log 2>&1
ONEBOX_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -race -timeout=10m -run '^TestRetentionOneProcessPipeline$' -v ./cmd/vector-server > reports/retention-onebox-pipeline.log 2>&1
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl
python3 scripts/verify_api.py > reports/acceptance-http.log
python3 scripts/verify_partitions.py > reports/partitions-http.log 2>&1
python3 scripts/verify_cluster.py > reports/cluster-http.log 2>&1
python3 scripts/verify_roles.py > reports/roles-http.log 2>&1
python3 -m unittest discover -s scripts -p test_cluster_local.py -v > reports/multiworker-unit.log 2>&1
python3 scripts/verify_multiworker.py > reports/multiworker-http.log 2>&1
python3 scripts/verify_replicas.py > reports/replicas-http.log 2>&1
python3 -m unittest discover -s scripts -p test_verify_balance.py -v > reports/balance-harness-unit.log 2>&1
python3 scripts/verify_balance.py > reports/balance-http.log 2>&1
python3 scripts/verify_cli.py > reports/cli-http.log 2>&1
PYTHONPATH=sdk/python .venv/bin/python -m unittest discover -s sdk/python/tests -v > reports/python-sdk-unit.log 2>&1
.venv/bin/python scripts/verify_rpc.py > reports/rpc-http.log 2>&1
.venv/bin/python scripts/verify_subscriptions.py > reports/subscriptions.log 2>&1
.venv/bin/python scripts/verify_retention.py > reports/retention-processes.log 2>&1
.venv/bin/python scripts/verify_hnsw_crash.py > reports/hnsw-crash.log 2>&1
.venv/bin/python -m unittest -v test_storage.py > reports/python-unit.log 2>&1
.venv/bin/python verify.py > reports/acceptance-s3.log
if [[ "${EXTENDED_ACCEPTANCE:-0}" == 1 ]]; then
  CAPACITY_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -run TestCapacityAcceptance -v ./internal/vector > reports/capacity.log 2>&1
  STRUCTURED_ACCEPTANCE=1 bash scripts/go.sh test -count=1 -run TestStructuredAcceptance -v ./internal/vector > reports/structured.log 2>&1
  bash scripts/native-check.sh
fi
printf 'passed\n' > reports/acceptance.status
