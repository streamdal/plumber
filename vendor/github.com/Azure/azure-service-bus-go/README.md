**Please note, a newer package is available: [azservicebus](https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/messaging/azservicebus/README.md) as of [2021-11-12].**
**We strongly encourage you to upgrade. See the [Migration Guide](https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/messaging/azservicebus/migrationguide.md/) for more details.**

# Microsoft Azure Service Bus Client for Golang
[![Go Report Card](https://goreportcard.com/badge/github.com/Azure/azure-service-bus-go)](https://goreportcard.com/report/github.com/Azure/azure-service-bus-go)
[![godoc](https://godoc.org/github.com/Azure/azure-service-bus-go?status.svg)](https://godoc.org/github.com/Azure/azure-service-bus-go)
[![Build Status](https://travis-ci.org/Azure/azure-service-bus-go.svg?branch=master)](https://travis-ci.org/Azure/azure-service-bus-go)
[![Coverage Status](https://coveralls.io/repos/github/Azure/azure-service-bus-go/badge.svg?branch=master)](https://coveralls.io/github/Azure/azure-service-bus-go?branch=master)

Microsoft Azure Service Bus is a reliable cloud messaging service (MaaS) which simplifies enterprise cloud messaging. It
enables developers to build scalable cloud solutions and implement complex messaging workflows over an efficient binary
protocol called AMQP.

This library provides a simple interface for sending, receiving and managing Service Bus entities such as Queues, Topics
and Subscriptions.

For more information about Service Bus, check out the [Azure documentation](https://azure.microsoft.com/en-us/services/service-bus/).

This library is a pure Golang implementation of Azure Service Bus over AMQP.

## Preview of Service Bus for Golang
This library is currently a preview. There may be breaking interface changes until it reaches semantic version `v1.0.0`. 
If you run into an issue, please don't hesitate to log a 
[new issue](https://github.com/Azure/azure-service-bus-go/issues/new) or open a pull request.

## Install using Go modules

``` bash
go get -u github.com/Azure/azure-service-bus-go
```

If you need to install Go, follow [the official instructions](https://golang.org/dl/)

### Examples

Find up-to-date examples and documentation on [godoc.org](https://godoc.org/github.com/Azure/azure-service-bus-go#pkg-examples).

### Running tests

Most tests require a properly configured service bus in Azure.  The easiest way to set this up is to use the [Terraform](https://www.terraform.io/) deployment script.
Running the integration tests will take longer than the default 10 mintues, please use a larger timeout `go test -timeout 30m`.

### Have questions?

The developers of this library are all active on the [Gopher Slack](https://gophers.slack.com), it is likely easiest to 
get our attention in the [Microsoft Channel](https://gophers.slack.com/messages/C6NH8V2E9). We'll also find your issue
if you ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/go+azure) with the tags `azure` and `go`.

## Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
