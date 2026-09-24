.DEFAULT_GOAL := build

LDFLAGS := $(shell sh bin/gen-build-stamp.sh)

.PHONY: gen_proto gen_api build build-cli test test-clients test-cli-matrix lint tidy clean run bench bench-m5 bench-m8 web test-web

gen_proto:
	sh bin/gen.sh

gen_api:
	sh bin/gen-api.sh

build: build-cli
	go build -ldflags "$(LDFLAGS)" -o ./ultima-server ./cmd/ultima-server

# web builds the M6d management UI (§10.2) into web/dist, which the next
# `make build` embeds. Requires bun. Without it, binaries embed the
# committed placeholder dist.
web:
	cd web && bun install && bun run build

test-web:
	cd web && bun run typecheck

test:
	go test ./...

# build-cli builds the three M7 operator CLIs (§6.4), thin shells over
# clients/go/ultima: ultima-cli (RESP), ultima-ws-cli, ultima-grpc-cli.
build-cli:
	go build -ldflags "$(LDFLAGS)" -o ./ultima-cli ./cmd/ultima-cli
	go build -ldflags "$(LDFLAGS)" -o ./ultima-ws-cli ./cmd/ultima-ws-cli
	go build -ldflags "$(LDFLAGS)" -o ./ultima-grpc-cli ./cmd/ultima-grpc-cli

# test-clients: Go client tests plus the TypeScript client typecheck and
# the JS distribution build + load smokes (the TS round-trip against a
# live server runs inside `go test ./tests/`, skipping gracefully without
# bun).
test-clients:
	go test ./clients/... -count=1
	cd clients/typescript && bun install --silent && bun run typecheck
	cd clients/javascript && bun install --silent && bun run build && bun run smoke

# test-cli-matrix runs the CLI command matrix (tests/cli-matrix) — every
# implemented Redis 7.2.7 command exercised against a live ultima-server
# through redis-cli and all three operator CLIs, in both security modes.
# Requires redis-cli and GNU timeout (brew install coreutils). Pass -R
# (MATRIX_FLAGS=-R) to also validate expectations against a real
# redis-server. See docs/cli-matrix-testing.md.
MATRIX_FLAGS ?=
test-cli-matrix: build build-cli
	sh bin/test-cli-matrix.sh $(MATRIX_FLAGS)

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -f ./ultima-server ./ultima-cli ./ultima-ws-cli ./ultima-grpc-cli

run: build
	./ultima-server --cfg ultima.cfg.json

bench:
	sh bin/bench.sh

bench-m5:
	sh bin/bench-m5.sh

bench-m8:
	sh bin/bench-m8.sh
