VERSION ?= $(shell git rev-parse --short HEAD)
BINARY   = plumber

GO = CGO_ENABLED=$(CGO_ENABLED) GOFLAGS=-mod=vendor go
CGO_ENABLED ?= 0
GO_BUILD_FLAGS = -ldflags "-X main.Version=${VERSION}"

# Pattern #1 example: "example : description = Description for example target"
# Pattern #2 example: "### Example separator text
help: HELP_SCRIPT = \
	if (/^([a-zA-Z0-9-\.\/]+).*?: description\s*=\s*(.+)/) { \
		printf "\033[34m%-40s\033[0m %s\n", $$1, $$2 \
	} elsif(/^\#\#\#\s*(.+)/) { \
		printf "\033[33m>> %s\033[0m\n", $$1 \
	}

.PHONY: help
help:
	@perl -ne '$(HELP_SCRIPT)' $(MAKEFILE_LIST)

### Dev

.PHONY: setup/linux
setup/linux: description = Install dev tools for linux
setup/linux:
	GO111MODULE=off go get github.com/maxbrunsfeld/counterfeiter

.PHONY: setup/darwin
setup/darwin: description = Install dev tools for darwin
setup/darwin:
	GO111MODULE=off go get github.com/maxbrunsfeld/counterfeiter

.PHONY: run
run: description = Run $(BINARY)
run:
	$(GO) run `ls -1 *.go | grep -v _test.go` -d

.PHONY: start/deps
start/deps: description = Start dependencies
start/deps:
	docker-compose up -d rabbitmq kafka

### Build

.PHONY: build
build: description = Build $(BINARY)
build: clean build/linux build/darwin

.PHONY: build/linux
build/linux: description = Build $(BINARY) for linux
build/linux: clean
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-linux

.PHONY: build/darwin
build/darwin: description = Build $(BINARY) for darwin
build/darwin: clean
	GOOS=darwin GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-darwin

.PHONY: clean
clean: description = Remove existing build artifacts
clean:
	$(RM) ./build/$(BINARY)-*

### Test

.PHONY: test
test: description = Run Go unit tests
test: GOFLAGS=
test:
	$(GO) test ./...

.PHONY: testv
testv: description = Run Go unit tests (verbose)
testv: GOFLAGS=
testv:
	$(GO) test ./... -v

.PHONY: test/functional
test/functional: description = Run functional tests
test/functional: GOFLAGS=
test/functional:
	$(GO) test ./... --tags=functional
