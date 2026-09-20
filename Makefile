.DEFAULT_GOAL := build

LDFLAGS := $(shell sh bin/gen-build-stamp.sh)

.PHONY: gen_proto gen_api build test lint tidy clean run bench bench-m5 web test-web

gen_proto:
	sh bin/gen.sh

gen_api:
	sh bin/gen-api.sh

build:
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

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -f ./ultima-server

run: build
	./ultima-server --cfg ultima.cfg.json

bench:
	sh bin/bench.sh

bench-m5:
	sh bin/bench-m5.sh
