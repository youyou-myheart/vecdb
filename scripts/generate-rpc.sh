#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Tools: grpcio-tools 1.76.0, protoc-gen-go 1.30.0, protoc-gen-go-grpc 1.3.0.
PATH="$PWD/.tools:$PATH" .venv/bin/python -m grpc_tools.protoc -I proto \
  --go_out=. --go_opt=module=example.com/vector-storage \
  --go-grpc_out=. --go-grpc_opt=module=example.com/vector-storage \
  --python_out=sdk/python --grpc_python_out=sdk/python \
  proto/vectorstore/v1/vector.proto
