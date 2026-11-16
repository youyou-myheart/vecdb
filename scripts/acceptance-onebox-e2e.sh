#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports .tools
bash scripts/go.sh build -o .tools/vector-server ./cmd/vector-server
bash scripts/go.sh build -o .tools/vectorctl ./cmd/vectorctl
exec .venv/bin/python scripts/onebox_e2e.py "$@"
