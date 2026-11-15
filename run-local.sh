#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p .state/seaweed
exec .tools/weed server -dir=.state/seaweed \
  -ip=127.0.0.1 -ip.bind=127.0.0.1 \
  -master.port=19333 -volume.port=18080 -filer.port=18888 \
  -s3.port=18333 -volume.max=8 -master.volumeSizeLimitMB=128 \
  -master.telemetry=false -s3 -s3.config=s3.json
