#!/usr/bin/env bash
# Prints the -ldflags -X stamp for cmd/ultima-server/version.go
# (design doc §8). Consumed by the Makefile's build target.
set -euo pipefail

cd "$(dirname "$0")/.."

git_commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)
git_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
build_date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
build_target=$(go env GOOS)/$(go env GOARCH)

printf -- '-X main.GitCommit=%s -X main.Version=%s -X main.BuildDate=%s -X main.GitBranchName=%s -X main.BuildTarget=%s' \
	"$git_commit" "$version" "$build_date" "$git_branch" "$build_target"
