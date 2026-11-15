#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .state/pd
exec .tools/tiup-home/components/pd/v8.5.8/pd-server \
  --name=vector-pd --data-dir=.state/pd \
  --client-urls=http://127.0.0.1:12379 --advertise-client-urls=http://127.0.0.1:12379 \
  --peer-urls=http://127.0.0.1:12380 --advertise-peer-urls=http://127.0.0.1:12380 \
  --log-file=.state/pd.log
