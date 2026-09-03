.DEFAULT_GOAL := build

LDFLAGS := $(shell sh bin/gen-build-stamp.sh)

.PHONY: gen_proto build test lint tidy clean run

gen_proto:
	sh bin/gen.sh

build:
	go build -ldflags "$(LDFLAGS)" -o ./ultima-server ./cmd/ultima-server

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
