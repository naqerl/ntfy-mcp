SHELL := /bin/bash

BINARY   := ntfy-mcp
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.VERSION=$(VERSION)
GO       ?= go
DIST     := bin

.PHONY: all fmt vet test build build-linux docker smoke clean

all: vet test build

fmt:
	$(GO) fmt ./...

vet:
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then echo "gofmt required for:"; echo "$$files"; exit 1; fi
	$(GO) vet ./...
	$(GO) tool staticcheck ./...

test:
	$(GO) test -race ./...

build:
	mkdir -p $(DIST)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST)/$(BINARY) .

build-linux:
	mkdir -p $(DIST)
	for arch in amd64 arm64; do \
		CGO_ENABLED=0 GOOS=linux GOARCH=$$arch $(GO) build -trimpath \
			-ldflags="$(LDFLAGS)" -o $(DIST)/$(BINARY)-linux-$$arch . || exit 1; \
	done

docker:
	docker build --build-arg VERSION=$(VERSION) -t ntfy-mcp:$(VERSION) -t ntfy-mcp:latest .

# The smoke script assumes a running gateway and ntfy. CI wires both up.
smoke:
	MCP_URL=$${MCP_URL:-http://127.0.0.1:8080/mcp} ./scripts/smoke.sh

clean:
	rm -rf $(DIST)
