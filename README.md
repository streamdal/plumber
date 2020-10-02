plumber
=======

[![Master build status](https://github.com/batchcorp/plumber/workflows/master/badge.svg)]() [![Go Report Card](https://goreportcard.com/badge/github.com/batchcorp/plumber)]()

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

**Getting fancy**: Decoding protobuf encoded messages and viewing them live

```bash
$ plumber read rabbit --address="amqp://localhost" --exchange events --routing-key \# \
  --line-numbers --output-type protobuf --protobuf-dir ~/schemas \
  --protobuf-root-message Message --follow
1: {"some-attribute": 123, "numbers" : [1, 2, 3]}
2: {"some-attribute": 424, "numbers" : [325]}
3: {"some-attribute": 49, "numbers" : [958, 288, 289, 290]}
4: ERROR: Cannot decode message as protobuf "Message"
5: {"some-attribute": 394, "numbers" : [4, 5, 6, 7, 8]}
^C
```

<sub>
If your schemas are located in multiple places, you can specify `--protobuf-dir`
multiple times. Treat it the same as you would `protoc -I`.
</sub> 

**Get your leisure suit on**: Capture and relay data from messaging systems and
send to the [Batch](https://batch.sh) platform:

```bash
# Run the relay in the foreground
$ plumber relay aws-sqs --queue-name TestQueue --collect-token 36e0ab54-1296-4db5-8fb8-e5fe1b54d3aa
HTTP server listening on localhost:8080
Relay started

^C

# Run it in Docker
$ docker run -d --name plumber-sqs -p 8080:8080 \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
    -e PLUMBER_RELAY_TYPE=aws-sqs \
    -e PLUMBER_RELAY_TOKEN=47b2b76e-0547-4dac-8b4b-74423fb02d53 \
    -e PLUMBER_RELAY_SQS_QUEUE_NAME=TestQueue
    batchcorp/plumber 
```

**Getting Help**

A full list of available flags can be displayed by using the `--help` flag after
different parts of the command:

```bash
$ plumber read message rabbit --help
$ plumber read message mqtt --help
$ plumber write message kafka --help
$ plumber relay --help
```

## Features

* Dynamic protobuf encode & decode
* Gzip decompress
* `--follow` support (ie. `tail -f`)
* Relay and archive all messaging system data
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
* NATS (coming soon)
* ActiveMQ (coming soon)
* Redis (coming soon)

NOTE: If your messaging tech is not supported - submit an issue and we'll do
our best to make it happen!

## Acknowledgments

**Huge** shoutout to [jhump](https://github.com/jhump) and for his excellent
[protoreflect](https://github.com/jhump/protoreflect) library, without which
`plumber` would not be anywhere *near* as easy to implement. _Thank you!_

## Contribute

We love contributions! Prior to sending us a PR, open an issue to discuss what
you intend to work on. When ready to open PR - add good tests and let's get this
thing merged!
