plumber
=======

plumber is a CLI devtool for inspecting, piping, massaging and redirecting data
in message systems like Kafka, RabbitMQ , GCP PubSub and 
[many more](#supported-messaging-systems). \[1]

The tool enables you to:

* See what's passing through your message systems
* Pipe data from one place to another
* Decode protobuf data in real-time
* Replay historical data (using [Batch](https://batch.sh))

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

Packages:

* [macOS](https://github.com/batchcorp/plumber/releases/download/v0.0.2/plumber-darwin)
* [Linux](https://github.com/batchcorp/plumber/releases/download/v0.0.2/plumber-linux)

Plumber is a single binary, to install you simply need to download it, give it executable
permissions and call it from your shell. Here's an example set of commands to do this:

```bash
# Put in the appropriate version and platform in the url, you can find the most up to date urls under
# Packages heading above

$ curl -o plumber https://github.com/batchcorp/plumber/releases/download/$VERSION/plumber-darwin
$ chmod +x plumber
$ mv plumber /usr/local/bin/plumber
```


## Usage

**Keep it simple**: Read & write messages

```bash
$ plumber read messages kafka --topic orders --with-line-numbers --follow
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

Read Stats:

Runtime: 5s
Events:  11
Rate:    2.2/s

$ plumber write message kafka --topic orders --input-data "plain-text"
Success! Wrote '1' message(s) to 'some-machine.domain.com'.
```

**Getting fancy**: Decoding protobuf encoded messages and viewing them live

```bash
$ plumber read messages rabbitmq --exchange events --routing-key \# \
  --line-numbers --output-type protobuf --protobuf-dir ~/schemas \
  --protobuf-root-message Message --follow
1: {"some-attribute": 123, "numbers" : [1, 2, 3]}
2: {"some-attribute": 424, "numbers" : [325]}
3: {"some-attribute": 49, "numbers" : [958, 288, 289, 290]}
4: ERROR: Cannot decode message as protobuf "Message"
5: {"some-attribute": 394, "numbers" : [4, 5, 6, 7, 8]}

^C

Read Stats:

Runtime: 5s
Events:  5
Rate:    1/s
```

**Get your leisure suit on (COMING SOON)** : Ability to create sinks and replay
data using the Batch platform: 

```bash
$ plumber create sink kafka --address some-machine.domain.com --topic orders --background
>> Launched plumber instance id 'abdea293-orders'

$ plumber get sinks
    [Name]                 [Host]           [Type]         [Online]         [Receieved] 
abdea293-orders      |  221.20.101.85  |     kafka    |     <1 mins    |       184,492

$ plumber create destination http --address http://localhost:8080 --name mikelaptop --batch --background
>> Launched destination id 'bfde92cd-laptop'

$ plumber get destinations
    [Name]                 [Host]          [Type]          [Online]         [Received]
bfde92cd-laptop    |  142.20.32.235  |      http       |   <1 mins      |       0
b593acbe-wabbit    |  142.20.32.238  |     rabbitmq    |   32 mins      |       0

$ plumber replay abdea293-orders --query '*' --destination bfde92cd-laptop
>> Replaying 1,202,293 matching events ...

[=========>---------------------------------------------]  17.00% 00m08s

>> Replay complete!

Replay Stats:

Total:       29m 13s
Rate:        73,200/minute
Success %:   99.8%
NumSuccess:  1,202,290
NumFailed:   3
```

## Features

* Dynamic protobuf encode & decode
* Gzip decompress
* `--follow` support (ie. `tail -f`)
* Single-binary, zero-config, easy-install

## Hmm, what is this Batch thing?

We are event sourcing enthusiasts that are working on a platform to enable folks
to replay events on their message busses.

While building our [company](https://batch.sh), we built a tool for reading and
writing messages from our message systems and realized that there is a serious
lack of tooling in this space.

We wanted a swiss army knife type of tool for working with messaging systems
(since we use Kafka and RabbitMQ internally) and so we created `plumber`.

## Supported Messaging Systems

* Kafka
* RabbitMQ
* Google Cloud Platform PubSub
* Amazon SQS (coming soon)
* NATS (coming soon)
* ActiveMQ (coming soon)
* Redis (coming soon)

## Acknowledgments

**Huge** shoutout to [jhump](https://github.com/jhump) and for his excellent
[protoreflect](https://github.com/jhump/protoreflect) library, without which
`plumber` would not be anywhere *near* as easy to implement. _Thank you!_

## Contribute

We love contributions! Prior to sending us a PR, open an issue to discuss what
you intend to work on. When ready to open PR - add good tests and let's get this
thing merged!
