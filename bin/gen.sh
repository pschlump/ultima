#!/usr/bin/env bash
# Regenerate Go protobuf bindings from proto/ into gen/go (design doc §6.2,
# §14.2). Requires protoc, protoc-gen-go and protoc-gen-go-grpc on PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p gen/go

protoc \
	--go_out=gen/go --go_opt=module=github.com/pschlump/ultima/gen/go \
	--go-grpc_out=gen/go --go-grpc_opt=module=github.com/pschlump/ultima/gen/go \
	proto/ultima/v1/command.proto \
	proto/ultima/v1/ping.proto
