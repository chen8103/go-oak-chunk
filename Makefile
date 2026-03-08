GO ?= go
BINARY_NAME ?= goc
CMD_PATH := ./cmd/go-oak-chunk
VARS_PKG := github.com/SisyphusSQ/go-oak-chunk/v3/vars

BUILD_FLAGS  = -X '${VARS_PKG}.AppName=${BINARY_NAME}'
# BUILD_FLAGS += -X '${VARS_PKG}.AppVersion=$(shell git describe --tags --always 2>/dev/null || echo dev)'
BUILD_FLAGS += -X '${VARS_PKG}.GoVersion=$(shell $(GO) version)'
BUILD_FLAGS += -X '${VARS_PKG}.BuildTime=$(shell date +"%Y-%m-%d %H:%M:%S")'
BUILD_FLAGS += -X '${VARS_PKG}.GitCommit=$(shell git rev-parse HEAD 2>/dev/null || echo unknown)'
BUILD_FLAGS += -X '${VARS_PKG}.GitRemote=$(shell git config --get remote.origin.url 2>/dev/null || echo unknown)'

.PHONY: all release build linux darwin test vet race deploy run clean

all: clean build test run

release: clean linux darwin test

build:
	$(GO) build -ldflags="$(BUILD_FLAGS)" -o $(BINARY_NAME) $(CMD_PATH)

linux:
	GOOS=linux GOARCH=amd64 $(GO) build -ldflags="$(BUILD_FLAGS)" -o $(BINARY_NAME).linux.amd64 $(CMD_PATH)

darwin:
	GOOS=darwin GOARCH=arm64 $(GO) build -ldflags="$(BUILD_FLAGS)" -o $(BINARY_NAME).darwin.arm64 $(CMD_PATH)

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

race:
	$(GO) test -race ./...

deploy: build
	@mv -f $(BINARY_NAME) /usr/local/bin/

run: build
	@./$(BINARY_NAME) version

clean:
	@$(GO) clean
	@rm -f $(BINARY_NAME)
	@rm -f $(BINARY_NAME).linux.amd64
	@rm -f $(BINARY_NAME).darwin.arm64