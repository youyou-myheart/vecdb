#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOPATH="$PWD/.tools/gopath"
export GOCACHE="$PWD/.tools/gocache"
export GOTOOLCHAIN=go1.26.10
# An Intel Python or Go launcher can report x86_64 on Apple Silicon. Ask the
# kernel if it can run the native slice. Explicit cross-builds keep their target.
if [[ "$(uname -s)" == Darwin && -z "${GOARCH:-}" && "${GOOS:-darwin}" == darwin ]] &&
   /usr/bin/arch -arm64 /usr/bin/true 2>/dev/null; then
  export GOARCH=arm64
  export CGO_ENABLED="${CGO_ENABLED:-1}"
fi
# Apple clang 14 cannot compile libc++ from the macOS 15 SDK.
# Keep the workaround local; callers can explicitly select another SDK.
if [[ "$(uname -s)" == Darwin && -z "${SDKROOT:-}" ]] && clang --version | head -1 | grep -q 'clang version 14\.'; then
  if [[ -d /Library/Developer/CommandLineTools/SDKs/MacOSX13.3.sdk ]]; then
    export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX13.3.sdk
    export MACOSX_DEPLOYMENT_TARGET=13.3
  fi
fi
exec go "$@"
