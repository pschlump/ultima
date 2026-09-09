#!/bin/sh
# Regenerate the chi-server bindings for the management HTTP API
# (design doc §10.1, D7/D11) from api/openapi.yaml into gen/httpapi/.
# Requires oapi-codegen v2 (`go install
# github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest`).
set -e
cd "$(dirname "$0")/.."
if ! command -v oapi-codegen >/dev/null 2>&1; then
    echo "oapi-codegen not found; install with:" >&2
    echo "  go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest" >&2
    exit 1
fi
oapi-codegen -config api/oapi-codegen.yaml api/openapi.yaml
echo "gen/httpapi/httpapi.gen.go regenerated"
# The spec is embedded into the binary and served at /api/openapi.yaml
# (D7); lib/httpapi/openapi.yaml is the embed copy, kept in lockstep here.
mkdir -p lib/httpapi
cp api/openapi.yaml lib/httpapi/openapi.yaml
echo "lib/httpapi/openapi.yaml synced"
