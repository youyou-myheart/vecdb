#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports
printf 'running\n' > reports/dispatch-acceptance.status
trap 'printf "failed\n" > reports/dispatch-acceptance.status' EXIT
.venv/bin/python scripts/quality.py --lane smoke
printf 'passed\n' > reports/dispatch-acceptance.status
trap - EXIT
