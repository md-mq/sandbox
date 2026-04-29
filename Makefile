BINARY := plx-exec
PKG    := ./cmd/plx-exec

VERSION ?= dev
ARCH    ?= $(shell go env GOARCH)

GO_BUILD_FLAGS := -trimpath
GO_LDFLAGS     := -s -w -X main.version=$(VERSION)

.PHONY: build build-static test lint run tidy clean

build:
	go build $(GO_BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" -o bin/$(BINARY) $(PKG)

# Static linux build for the init image. Override ARCH=arm64 for cross.
build-static:
	CGO_ENABLED=0 GOOS=linux GOARCH=$(ARCH) \
	    go build $(GO_BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" \
	    -o bin/$(BINARY)-linux-$(ARCH) $(PKG)

test:
	go test -race -count=1 ./...

lint:
	go vet ./...

run:
	POLYAXON_SANDBOX_PING_ONLY=1 go run $(PKG)

tidy:
	go mod tidy

clean:
	rm -rf bin/
