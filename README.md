<img src="https://github.com/batchcorp/plumber/blob/master/assets/gopher.png?raw=true" align="right" />

plumber
=======

[![Master build status](https://github.com/batchcorp/plumber/workflows/master/badge.svg)](https://github.com/batchcorp/p) [![Go Report Card](https://goreportcard.com/badge/github.com/batchcorp/plumber)](https://goreportcard.com/badge/github.com/batchcorp/plumber)

plumber is a CLI devtool for inspecting, piping, massaging and redirecting data
in message systems like Kafka, RabbitMQ , GCP PubSub and 
[many more](#supported-messaging-systems). \[1]

The tool enables you to:

* See what's passing through your message systems
* Pipe data from one place to another
* Decode protobuf data in real-time
* Capture and relay data to [Batch platform](https://batch.sh)

<sub>\[1] It's like `curl` for messaging systems.</sub>

## Why do you need it?

Messaging systems are black boxes - gaining visibility into what is passing
through them is an involved process that requires you to write consumer code
that you will likely throw away.

`plumber` enables you to stop wasting time writing throw-away code - use it to
look into your queues, use it to connect disparate systems together or use it
for debugging your event driven systems.

## Demo

![Brief Demo](./assets/demo.gif)

## Install

### Via brew

```bash
$ brew tap batchcorp/public
$ brew install plumber
```

### Manually

* [macOS](https://github.com/batchcorp/plumber/releases/latest/download/plumber-darwin)
* [Linux](https://github.com/batchcorp/plumber/releases/latest/download/plumber-linux)
* [Windows](https://github.com/batchcorp/plumber/releases/latest/download/plumber-windows.exe)

Plumber is a single binary, to install you simply need to download it, give it executable
permissions and call it from your shell. Here's an example set of commands to do this:

```bash
$ curl -L -o plumber https://github.com/batchcorp/plumber/releases/latest/download/plumber-darwin
$ chmod +x plumber
$ mv plumber /usr/local/bin/plumber
```

## Usage

**Keep it simple**: Read & write messages

```bash
$ plumber read kafka --topic orders --address="some-machine.domain.com:9092" --line-numbers --follow
1: {"sample" : "message 1"}
2: {"sample" : "message 2"}
3: {"sample" : "message 3"}
4: {"sample" : "message 4"}
5: {"sample" : "message 5"}
6: {"sample" : "message 6"}
7: {"sample" : "message 7"}
8: {"sample" : "message 8"}
9: {"sample" : "message 9"}
10: {"sample" : "message 10"}
11: {"sample" : "message 11"}
^C

$ plumber write kafka --address="some-machine.domain.com:9092" --topic orders --input-data "plain text"
Success! Wrote '1' message(s) to 'localhost:9092'.
```

<sub>NOTE: If you want to write JSON either surround the `input-data` in single
quotes or use `input-file`.


#### See [EXAMPLES.md](https://github.com/batchcorp/plumber/blob/master/EXAMPLES.md) for more usage examples
#### See [ENV.md](https://github.com/batchcorp/plumber/blob/master/ENV.md) for list of supported environment variables

## Getting Help

A full list of available flags can be displayed by using the `--help` flag after
different parts of the command:

```bash
$ plumber read rabbit --help
$ plumber read mqtt --help
$ plumber write kafka --help
$ plumber relay --help
```

## Features

* Dynamic protobuf & avro encode & decode
* Gzip compress & decompress
* `--follow` support (ie. `tail -f`)
* Observe, relay and archive messaging data
* Support for **most** messaging systems
* Single-binary, zero-config, easy-install

## Hmm, what is this Batch thing?

We are distributed system enthusiasts that started a company called
[Batch](https://batch.sh). We focus on improving workflows that involve
messaging systems - specifically, we enable message observability, backups and
outage recovery via message replays.

While working on our company, we built a tool for reading and writing messages
from our message systems and realized that there is a serious lack of tooling
in this space.

We wanted a swiss army knife type of tool for working with messaging systems
(we use Kafka and RabbitMQ internally), so we created `plumber`.

## Why the name `plumber`?

We consider ourselves "internet plumbers" of sort - so the name seemed to fit :)

## Supported Messaging Systems

* Kafka
* RabbitMQ
* Google Cloud Platform PubSub
* MQTT
* Amazon SQS
* Amazon SNS (Publishing)
* ActiveMQ
* Azure Service Bus 
* NATS
* Redis-PubSub
* Redis-Streams

NOTE: If your messaging tech is not supported - submit an issue and we'll do
our best to make it happen!

## High Availability
When running `plumber` in relay mode in production, you will want to run at
least 2 instances of `plumber` - that way updates, maintenances or unexpected
problems will not interfere with data collection.

You can achieve H/A by launching 2+ instances of plumber with identical 
configurations.

### Kafka
You need to ensure that you are using the same consumer group on all plumber
instances.

### RabbitMQ
Make sure that all instances of `plumber` are pointed to the same queue.

### Note on boolean flags
In order to flip a boolean flag to `false`, prepend `--no` to the flag.

ie. `--queue-declare` is `true` by default. To make it false, use `--no-queue-declare`.

## Acknowledgments

**Huge** shoutout to [jhump](https://github.com/jhump) and for his excellent
[protoreflect](https://github.com/jhump/protoreflect) library, without which
`plumber` would not be anywhere *near* as easy to implement. _Thank you!_

## Release

To push a new plumber release:

1. `git tag v0.18.0 master`
1. `git push origin v0.18.0`
1. Watch the github action
1. New release should be automatically created under https://github.com/batchcorp/plumber/releases/
1. Update release to include any relevant info

## Contribute

We love contributions! Prior to sending us a PR, open an issue to discuss what
you intend to work on. When ready to open PR - add good tests and let's get this
thing merged!
