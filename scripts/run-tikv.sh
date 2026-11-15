#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .state/tikv
exec .tools/tiup-home/components/tikv/v8.5.8/tikv-server \
  --config=tikv-local.toml \
  --addr=127.0.0.1:12160 --status-addr=127.0.0.1:12180 \
  --pd=127.0.0.1:12379 --data-dir=.state/tikv --log-file=.state/tikv.log
