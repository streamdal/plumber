VERSION ?= $(shell git rev-parse --short HEAD)
SHORT_SHA ?= $(shell git rev-parse --short HEAD)
GIT_TAG ?= $(shell git describe --tags --abbrev=0)
BINARY   = plumber
ARCH ?= $(shell uname -m)

GO = CGO_ENABLED=$(CGO_ENABLED) GONOPROXY=github.com/batchcorp GOFLAGS=-mod=vendor go
CGO_ENABLED ?= 0
GO_BUILD_FLAGS = -ldflags "-X 'github.com/batchcorp/plumber/options.VERSION=${VERSION}' -X 'main.TELEMETRY_API_KEY=${TELEMETRY_API_KEY}'"

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
	$(GO) run `ls -1 *.go | grep -v _test.go`

.PHONY: start/deps
start/deps: description = Start dependencies
start/deps:
	docker-compose up -d rabbitmq kafka

### Build

.PHONY: build
build: description = Build $(BINARY)
build: clean build/linux build/darwin build/darwin-arm64 build/windows

.PHONY: build/linux
build/linux: description = Build $(BINARY) for linux
build/linux: clean
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-linux

.PHONY: build/linux-amd64
build/linux-amd64: description = Build $(BINARY) for linux
build/linux-amd64: clean
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-linux-amd64

	
.PHONY: build/linux-x86_64
build/linux-x86_64: description = Build $(BINARY) for linux
build/linux-x86_64: clean
	GOOS=linux GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-linux-amd64

.PHONY: build/linux-arm64
build/linux-arm64: description = Build $(BINARY) for linux
build/linux-arm64: clean
	GOOS=linux GOARCH=arm64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-linux-arm64

.PHONY: build/darwin
build/darwin: description = Build $(BINARY) for darwin
build/darwin: clean
	GOOS=darwin GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-darwin

.PHONY: build/darwin-arm64
build/darwin-arm64: description = Build $(BINARY) for darwin/arm64 (Apple Silicon)
build/darwin-arm64: clean
	GOOS=darwin GOARCH=arm64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-darwin-arm64

.PHONY: build/darwin-amd64
build/darwin-amd64: description = Build $(BINARY) for darwin/amd64 (Intel)
build/darwin-amd64: clean
	GOOS=darwin GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-darwin-amd64

.PHONY: build/windows
build/windows: description = Build $(BINARY) for windows
build/windows: clean
	GOOS=windows GOARCH=amd64 $(GO) build $(GO_BUILD_FLAGS) -o ./build/$(BINARY)-windows.exe

.PHONY: clean
clean: description = Remove existing build artifacts
clean:
	$(RM) ./build/$(BINARY)-*

### Generation

.PHONY: generate/docs
generate/docs: description = Generate documentation
generate/docs:
	go run tools/docs-generator/main.go -type env -output markdown > docs/env.md

### Docker

docker/build: description = Build docker image
docker/build:
	docker buildx build --push --platform=linux/amd64,linux/arm64 \
	-t streamdal/$(BINARY):$(SHORT_SHA) \
	-t streamdal/$(BINARY):$(GIT_TAG) \
	-t streamdal/$(BINARY):latest \
	-t streamdal/$(BINARY):local \
	-f ./Dockerfile .

.PHONY: docker/build/local
docker/build/local: description = Build docker image
docker/build/local:
	docker build -t streamdal/$(SERVICE):$(VERSION) --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 \
	-t streamdal/$(SERVICE):latest \
	-f ./Dockerfile .

.PHONY: docker/push
docker/push: description = Push local docker image
docker/push:
	docker push streamdal/$(BINARY):$(SHORT_SHA) && \
	docker push streamdal/$(BINARY):$(GIT_TAG) && \
	docker push streamdal/$(BINARY):latest

.PHONY: docker/run
docker/run: description = Run local plumber in Docker
docker/run:
	docker run --name plumber -p 8080:8080 \
		-e PLUMBER_RELAY_TOKEN=48b30466-e3cb-4a58-9905-45b74284709f \
		-e PLUMBER_RELAY_GRPC_ADDRESS=localhost:9000 \
		-e PLUMBER_RELAY_GRPC_DISABLE_TLS=true \
		-e PLUMBER_RELAY_SQS_QUEUE_NAME=PlumberTestQueue \
		-e PLUMBER_RELAY_SQS_AUTO_DELETE=true \
		-e PLUMBER_DEBUG=true \
		-d streamdal/$(BINARY):local aws-sqs

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

.PHONY: test/coverage
test/coverage: description = Run Go unit tests and output coverage information
test/coverage: GOFLAGS=
test/coverage:
	$(GO) test ./... -coverprofile c.out

.PHONY: test/dev
test/dev: description = Run Go unit tests, output coverage information to a browser
test/dev: GOFLAGS=
test/dev:
	$(GO) test ./... -coverprofile c.out
	$(GO) tool cover -html=c.out -o cover.html
	open cover.html

.PHONY: test/functional
test/functional: description = Run functional tests
test/functional: GOFLAGS=
test/functional:
	$(GO) test ./... --tags=functional

.PHONE: test/fakes
test/fakes: description = Generate test fakes
test/fakes: GOFLAGS=
test/fakes:
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/pulsar/pulsarfakes/fake_pulsar.go github.com/apache/pulsar-client-go/pulsar.Client &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/pulsar/pulsarfakes/fake_producer.go github.com/apache/pulsar-client-go/pulsar.Producer &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/pulsar/pulsarfakes/fake_consumer.go github.com/apache/pulsar-client-go/pulsar.Consumer &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/pulsar/pulsarfakes/fake_message.go github.com/apache/pulsar-client-go/pulsar.Message &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/pulsar/pulsarfakes/fake_messageid.go github.com/apache/pulsar-client-go/pulsar.MessageID &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/mqtt/mqttfakes/fake_mqtt.go github.com/eclipse/paho.mqtt.golang.Client &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/rabbitmq/rabbitfakes/fake_rabbit.go github.com/batchcorp/rabbit.IRabbit &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/awssns/snsfakes/fake_sns.go github.com/aws/aws-sdk-go/service/sns/snsiface.SNSAPI &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/awssqs/sqsfakes/fake_sqs.go github.com/aws/aws-sdk-go/service/sqs/sqsiface.SQSAPI &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/awskinesis/kinesisfakes/fake_kinesis.go github.com/aws/aws-sdk-go/service/kinesis/kinesisiface.KinesisAPI &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/nats-streaming/stanfakes/fake_stan.go github.com/nats-io/stan.go.Conn &
	$(GO) run github.com/maxbrunsfeld/counterfeiter/v6 -o backends/nats-streaming/stanfakes/fake_subscription.go github.com/nats-io/stan.go.Subscription &
	$(GO) generate ./...
