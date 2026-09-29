GO ?= go
GOFLAGS ?= -buildvcs=false

.PHONY: test race build

test:
	cd go && $(GO) test ./...

race:
	cd go && $(GO) test -race ./...

build:
	mkdir -p dist
	cd go && $(GO) build $(GOFLAGS) -buildmode=c-shared -o ../dist/codex-overload-continue.so .
