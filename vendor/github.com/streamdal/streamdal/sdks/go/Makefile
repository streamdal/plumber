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

.PHONY: setup/darwin
setup/darwin: description = Install toolkit for building on macOS M1
setup/darwin:
	brew install tinygo wasmtime

.PHONY: test
test: description = Run all tests
test:
	bash ./init_wasm.sh
	go test ./... -v

.PHONY: test/fakes
test/fakes: description = Generate all fakes
test/fakes:
	go generate ./...


.PHONY: test/coverage
test/coverage: description = Run all tests
test/coverage:
	bash ./init_wasm.sh
	go test -coverprofile=coverage.out `go list ./... | grep -v fake`

.PHONY: test/benchmark
test/benchmark: description = Run all benchmarks
test/benchmark:
	bash ./init_wasm.sh
	go test -bench=. -run=^#

