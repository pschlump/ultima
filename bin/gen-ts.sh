#!/usr/bin/env bash
# Regenerate TypeScript protobuf bindings (protobuf-es v2) from proto/ into
# gen/ts (design doc §6.2, §14.2). Requires protoc on PATH and the
# protoc-gen-es plugin in tests/ts-roundtrip/node_modules/.bin — run
# `bun install` in tests/ts-roundtrip first. When the plugin is missing this
# script prints a hint and exits 0 so Go-only contributors are not broken.
set -euo pipefail

cd "$(dirname "$0")/.."

PLUGIN="tests/ts-roundtrip/node_modules/.bin/protoc-gen-es"

if [[ ! -x "$PLUGIN" ]]; then
	echo "gen-ts: $PLUGIN not found — skipping TypeScript generation." >&2
	echo "gen-ts: run 'bun install' in tests/ts-roundtrip to install @bufbuild/protoc-gen-es, then re-run." >&2
	exit 0
fi

# protoc resolves imports from the repo root (ping.proto imports
# "proto/ultima/v1/command.proto"), so output lands under a proto/ prefix;
# stage and flatten into gen/ts/ultima/v1. The generated cross-file imports
# are relative ("./command_pb.js"), so flattening is safe.
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

protoc \
	--es_out="$STAGE" --es_opt=target=ts \
	--plugin="protoc-gen-es=$PLUGIN" \
	proto/ultima/v1/command.proto \
	proto/ultima/v1/ping.proto

rm -rf gen/ts
mkdir -p gen/ts/ultima/v1
cp "$STAGE"/proto/ultima/v1/*.ts gen/ts/ultima/v1/
