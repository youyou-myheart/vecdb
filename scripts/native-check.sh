#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p reports .tools
printf "running\n" > reports/native-tsan.status
trap 'printf "failed\n" > reports/native-tsan.status' ERR
if [[ "$(uname -s)" == Darwin && -z "${SDKROOT:-}" ]] && clang --version | head -1 | grep -q 'clang version 14\.'; then
  export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX13.3.sdk
fi
"${CXX:-clang++}" -std=c++14 -O1 -g -fsanitize=thread -Iinternal/ann \
  internal/ann/bridge.cc tests/native_stress.cc -o .tools/ann-tsan > reports/native-tsan.log 2>&1
.tools/ann-tsan >> reports/native-tsan.log 2>&1
printf 'passed\n' > reports/native-tsan.status
