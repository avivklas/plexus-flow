#!/usr/bin/env bash
# Regenerate the gRPC stubs from the canonical protocol definition in the Go tree.
set -euo pipefail
cd "$(dirname "$0")/.."
cp ../../pkg/remote/workerpb/worker.proto src/plexus_flow/_proto/worker.proto
uv run python -m grpc_tools.protoc -I src \
    --python_out=src --grpc_python_out=src --mypy_out=src --mypy_grpc_out=src \
    src/plexus_flow/_proto/worker.proto
