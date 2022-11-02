# Plumber Usage Examples

  * [Consuming](#consuming)
       * [AWS SQS](#aws-sqs)
       * [RabbitMQ](#rabbitmq)
       * [RabbitMQ Streams](#rabbitmq-streams)
       * [Kafka](#kafka)
       * [Azure Service Bus](#azure-service-bus)
       * [Azure Event Hub](#azure-event-hub)
       * [NATS](#nats)
       * [NATS Streaming](#nats-streaming)
       * [NATS JetStream](#nats-jetstream)
       * [Redis PubSub](#redis-pubsub)
       * [Redis Streams](#redis-streams)
       * [GCP Pub/Sub](#gcp-pubsub)
       * [Postgres CDC](#cdc-postgres)
       * [MQTT](#mqtt)
       * [Apache Pulsar](#apache-pulsar)
       * [NSQ](#nsq)
       * [Thrift Decoding with IDL files](#thrift-decoding-with-idl-files)
       * [AWS Kinesis](#aws-kinesis)
  * [Publishing](#publishing)
       * [AWS SQS](#aws-sqs-1)
       * [AWS SNS](#aws-sns)
       * [RabbitMQ](#rabbitmq-1)
       * [RabbitMQ Streams](#rabbitmq-streams-1)
       * [Kafka](#kafka-1)
       * [Azure Service Bus](#azure-service-bus-1)
       * [Azure Event Hub](#azure-event-hub-1)
       * [NATS](#nats-1)
       * [NATS Streaming](#nats-streaming-1)
       * [NATS JetStream](#nats-jetstream-1)
       * [Redis PubSub](#redis-pubsub-1)
       * [Redis Streams](#redis-streams-1)
       * [GCP Pub/Sub](#gcp-pubsub-1)
       * [MQTT](#mqtt-1)
       * [Apache Pulsar](#apache-pulsar-1)
       * [NSQ](#nsq-1)
       * [AWS Kinesis](#aws-kinesis-1)
  * [Relay Mode](#relay-mode)
       * [Continuously relay messages from your RabbitMQ instance to a Batch.sh collection](#continuously-relay-messages-from-your-rabbitmq-instance-to-a-batchsh-collection)
       * [Continuously relay messages from an SQS queue to a Batch.sh collection](#continuously-relay-messages-from-an-sqs-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure queue to a Batch.sh collection](#continuously-relay-messages-from-an-azure-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure topic to a Batch.sh collection](#continuously-relay-messages-from-an-azure-topic-to-a-batchsh-collection)
       * [Continuously relay messages for multiple Redis channels to a Batch.sh collection](#continuously-relay-messages-from-multiple-redis-channels-to-a-batchsh-collection)
       * [Continuously relay messages for multiple Redis streams to a Batch.sh collection](#continuously-relay-messages-from-multiple-redis-streams-to-a-batchsh-collection)
       * [Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)](#continuously-relay-messages-from-a-kafka-topic-on-confluent-to-a-batchsh-collection-via-cli)
       * [Continuously relay messages from a MQTT topic to a Batch.sh collection](#continuously-relay-messages-from-a-mqtt-topic-to-a-batchsh-collection)
       * [Continuously relay messages from a NATS JetStream stream to a Batch.sh collection](#continuously-relay-messages-from-a-nats-jetstream-stream-to-a-batchsh-collection)
  * [Change Data Capture](#change-data-capture)
       * [Continuously relay Postgres change events to a Batch.sh collection](#continuously-relay-postgres-change-events-to-a-batchsh-collection)
       * [Continuously relay MongoDB change stream events to a Batch.sh collection](#continuously-relay-mongodb-change-stream-events-to-a-batchsh-collection)
  * [Advanced Usage](#advanced-usage)
       * [Decoding protobuf encoded messages and viewing them live](#decoding-protobuf-encoded-messages-and-viewing-them-live)
       * [Shallow envelope protobuf messages](#shallow-envelope-protobuf-messages)
       * [Using File Descriptor Sets](#using-file-descriptor-sets)
       * [Using Avro schemas when reading or writing](#using-avro-schemas-when-reading-or-writing)

## Consuming

##### AWS SQS

Read X number of messages
```
plumber read aws-sqs --queue-name=orders --max-num-messages=10
```

Read messages and delete them afterwards

```
plumber read aws-sqs --queue-name=orders --max-num-messages=10 --auto-delete
```

Continuously read messages

```
plumber read aws-sqs --queue-name=orders --continuous
```

Poll for new messages for X seconds

```
plumber read aws-sqs --queue-name=orders --wait-time-seconds=20
```

##### RabbitMQ

```
plumber read rabbit \
    --address="amqp://localhost:5672" \
    --exchange-name=testex \
    --queue-name=testqueue \
    --binding-key="orders.#" \
    --continuous
```

##### RabbitMQ Streams

```
plumber read rabbit-streams --dsn rabbitmq-stream://guest:guest@localhost:5552 --stream new_orders --offset last
```

##### Kafka

Read a single message

```
plumber read kafka --topics orders --address="broker1.domain.com:9092"
```

You may specify multiple brokers by specifying the `--address` flag multiple times

```
plumber read kafka --topics orders \
    --address="broker1.domain.com:9092" \
    --address="broker2.domain.com:9092" \
    --address="broker3.domain.com:9092" \
    --continuous
```

Continuously read messages

```
plumber read kafka --topics orders --address="broker1.domain.com:9092" --continuous
```

To read from multiple topics, specify multiple topics delimited with a comma (without spaces):

```
plumber read kafka --topics one,two -f --pretty
```

##### Azure Service Bus

Reading from a topic

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber read azure-service-bus --topic="new-orders" --subscription="copy-of-new-orders"
```

Reading from a queue

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber read azure-service-bus --queue "new-orders"
```

##### Azure Event Hub

Read first available message from any partition

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber read azure-event-hub
```

##### NATS

```bash
plumber read nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject"
```

##### NATS Streaming

```bash
plumber read nats-streaming --address="nats://user:pass@nats.test.io:4222" --channel "orders" --cluster-id "test-cluster" --client-id "plumber"
```

##### NATS JetStream

```bash
plumber read nats-jetstream --dsn="nats://user:pass@nats.test.io:4222" --stream "orders.>" --client-id "plumber"
```

Create and use a durable consumer:

```bash
plumber read nats-jetstream --dsn="nats://user:pass@nats.test.io:4222" --stream foo --create-durable-consumer
```

Use an existing durable consumer:
```bash
plumber read nats-jetstream --dsn="nats://user:pass@nats.test.io:4222" --stream foo --existing-durable-consumer --consumer-name existing_consumer
```

Create a new durable consumer at a specific stream start sequence:

```bash
plumber read nats-jetstream --dsn="nats://user:pass@nats.test.io:4222" --stream foo --create-durable-consumer --consumer-start-sequence 42
```

NOTE: By default, `plumber` will remove any consumers it creates. To leave consumers untouched, set `--keep-consumer`.

##### Redis PubSub

```bash
plumber read redis-pubsub --address="localhost:6379" --channels="new-orders"
```

##### Redis Streams

```bash
plumber read redis-streams --address="localhost:6379" --streams="new-orders"
```

##### GCP Pub/Sub

```bash
plumber read gcp-pubsub --project-id=PROJECT_ID --subscription-id=SUBSCRIPTION
```

##### MQTT

```bash
plumber read mqtt --address tcp://localhost:1883 --topic iotdata --qos-level at_least_once

# Or connect with TLS:

plumber read mqtt --address ssl://localhost:8883 --topic iotdata --qos-level at_least_once

# TLS using certificates

plumber read mqtt --address ssl://localhost:8883 --topic iotdata --qos-level at_least_once --tls-ca-cert=/path/to/ca_certificate.pem --tls-client-key=/path/to/client_key.pem --tls-client-cert=/path/to/client_certificate.pem
```

#### Apache Pulsar

```bash
plumber read pulsar --topic NEWORDERS --name plumber
```

#### NSQ

```bash
plumber read nsq --lookupd-address localhost:4161 --topic orders --channel neworders 
```

#### Thrift Decoding

> **_NOTE:_** This method is deprecated. See [Thrift Decoding with IDL Files](#thrift-decoding-with-idl-files) for an improved method of reading thrift

Plumber can decode thrift output, and display it as nested JSON. The key is the field's ID, and the
value is the actual value in the message. Add the `--pretty` flag to colorize output.

```bash
plumber read kafka --topics orders --decode-type thrift --pretty

{
  "1": 54392501,
  "2": "Test Order",
  "3": {
    "1": "Product Name",
    "2": "green",
    "3": "2091.99"
  }
}
```

#### Thrift Decoding with IDL files

**NEW** Support for decoding with IDL files

Plumber can now use your .thrift IDL files to decode the output with field/enum names

```bash
$ read kafka --topics thrifttest \
      --thrift-struct sh.batch.schema.Account \
      --thrift-dirs ./test-assets/thrift/schema/ \
      --decode-type thrift \
      --pretty
```

```json
{
  "emails": [
    "gopher@golang.com",
    "gopher2@golang.com"
  ],
  "id": 321,
  "model": {
    "includedvalue": "value of included struct"
  },
  "name": "Mark Gregan",
  "permissions": [
    "create",
    "read",
    "update",
    "delete"
  ],
  "price": 1.23,
  "subm": {
    "value": "submessage value here"
  },
  "teams": {
    "123": "554bf385-ce1f-4deb-9a99-8864c1df52b5"
  },
  "testconst": 1234,
  "type": "VIP",
  "unionthing": {
    "thing_int": null,
    "thing_string": "Daniel"
  }
}

```

#### AWS Kinesis

Reading from a single shard
```bash
plumber read kinesis --stream orders --shard shardId-000000000000 --latest --max-records 10
```

Read from all shards
```bash
plumber read kinesis --stream orders --continuous
```

## Publishing

##### AWS SQS

```
plumber write aws-sqs --queue-name=NewOrders --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### RabbitMQ

```
plumber write rabbit --address="amqp://rabbit.yourdomain.net:5672" --exchange-name=NewOrders --routing-key="orders.oregon.coffee" --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### RabbitMQ Streams

```
plumber write rabbit-streams --dsn rabbitmq-stream://guest:guest@localhost:5552 --stream new_orders --input '{"order_id": "A-3458-654-1", "status": "processed"}'
```

##### Kafka

```
plumber write kafka --address localhost:9092 --topics neworders --input '{"order_id": "A-3458-654-1", "status": "processed"}'
```

You may specify multiple brokers by specifying the `--address` flag multiple times.

To read from more than one topic, you may specify multiple `--topic` flags.

```
plumber write kafka --topics neworders \
    --address "broker1.domain.com:9092" \
    --address "broker2.domain.com:9092" \
    --address "broker3.domain.com:9092" \
    --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### AWS SNS

```bash
plumber write aws-sns --topic="arn:aws:sns:us-east-2:123456789012:MyTopic" --input="New event is ready for processing!"
```

##### Azure Service Bus

Publishing to a topic

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber write azure --topic="new-orders" --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

Publishing to a queue

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber write azure --queue="new-orders" --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Azure Event Hub

Publish to random partition

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber write azure-eventhub --input "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}" --message-id "neworder123"
```

Publish to specific partition key

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber write azure-eventhub --input "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}" --message-id "neworder123" --partition-key "neworders"
```

##### NATS

```bash
plumber write nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject" --input "Hello World"
```

##### NATS Streaming

```bash
plumber write nats-streaming --address="nats://user:pass@nats.test.io:4222" --channel "orders" --cluster-id "test-cluster" --client-id "plumber-producer" --input "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### NATS JetStream

```bash
plumber read nats-jetstream --dsn="nats://user:pass@nats.test.io:4222" --stream "orders.>" --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Redis PubSub

```bash
plumber write redis-pubsub --address="localhost:6379" --channels="new-orders" --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Redis Streams

```bash
plumber write redis-streams --address="localhost:6379" --streams="new-orders" --key foo --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### GCP Pub/Sub

```bash
plumber write gcp-pubsub --topic-id=TOPIC --project-id=PROJECT_ID --input='{"Sensor":"Room J","Temp":19}' 
```

##### MQTT

```bash
plumber write mqtt --address tcp://localhost:1883 --topic iotdata --qos-level at_least_once --input "{\"id\": 123, \"temperature\": 15}"

# or connect with TLS:

plumber write mqtt --address ssl://localhost:8883 --topic iotdata --qos-level at_least_once --input "{\"id\": 123, \"temperature\": 15}"

# TLS using certificates

plumber write mqtt --address ssl://localhost:8883 --topic iotdata --qos-level at_least_once --tls-ca-cert=/path/to/ca_certificate.pem --tls-client-key=/path/to/client_key.pem --tls-client-cert=/path/to/client_certificate.pem --input "{\"id\": 123, \"temperature\": 15}"
```

##### Apache Pulsar

```bash
plumber write pulsar --topic NEWORDERS --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### NSQ

```bash
plumber write nsq --nsqd-address localhost:4050 --topic orders --input="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### AWS Kinesis

```bash
plumber write kinesis --stream teststream --partition-key orders --input "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

## Relay Mode

##### Continuously relay messages from your RabbitMQ instance to a Batch.sh collection

```bash
$ docker run --name plumber-rabbit -p 8080:8080 \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    -e PLUMBER_RELAY_RABBIT_EXCHANGE=my_exchange \
    -e PLUMBER_RELAY_RABBIT_QUEUE=my_queue \
    -e PLUMBER_RELAY_RABBIT_ROUTING_KEY=some.routing.key \
    -e PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE=false \
    -e PLUMBER_RELAY_RABBIT_QUEUE_DURABLE=true \
    batchcorp/plumber plumber relay rabbit
```

##### Continuously relay messages from an SQS queue to a Batch.sh collection

```bash
docker run -d --name plumber-sqs -p 8080:8080 \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e PLUMBER_RELAY_SQS_QUEUE_NAME=TestQueue \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay aws-sqs
```

##### Continuously relay messages from an Azure queue to a Batch.sh collection

```bash
docker run -d --name plumber-azure -p 8080:8080 \
    -e SERVICEBUS_CONNECTION_STRING="Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName..."
    -e PLUMBER_RELAY_AZURE_QUEUE_NAME=neworders \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay azure-service-bus
```

##### Continuously relay messages from an Azure topic to a Batch.sh collection

```bash
docker run -d --name plumber-azure -p 8080:8080 \
    -e SERVICEBUS_CONNECTION_STRING="Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName..."
    -e PLUMBER_RELAY_AZURE_TOPIC_NAME=neworders \
    -e PLUMBER_RELAY_AZURE_SUBSCRIPTION=some-sub \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay azure-service-bus
```

##### Continuously relay messages from multiple Redis channels to a Batch.sh collection

```bash
docker run -d --name plumber-redis-pubsub -p 8080:8080 \
    -e PLUMBER_RELAY_REDIS_PUBSUB_ADDRESS=localhost:6379 \
    -e PLUMBER_RELAY_REDIS_PUBSUB_CHANNELS=channel1,channel2 \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay redis-pubsub
```

##### Continuously relay messages from multiple Redis streams to a Batch.sh collection

```bash
docker run -d --name plumber-redis-streams -p 8080:8080 \
    -e PLUMBER_RELAY_REDIS_STREAMS_ADDRESS=localhost:6379 \
    -e PLUMBER_RELAY_REDIS_STREAMS_STREAMS=stream1,stream2 \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay redis-streams
```

##### Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)

```
docker run -d --name plumber-kafka -p 8080:8080 \
    -e PLUMBER_RELAY_TOKEN="$YOUR-BATCHSH-TOKEN-HERE"
    -e PLUMBER_RELAY_KAFKA_ADDRESS="pkc-4kgmg.us-west-2.aws.confluent.cloud:9092,pkc-5kgmg.us-west-2.aws.confluent.cloud:9092"
    -e PLUMBER_RELAY_KAFKA_TOPIC="$YOUR_TOPIC"
    -e PLUMBER_RELAY_KAFKA_INSECURE_TLS="true"
    -e PLUMBER_RELAY_KAFKA_USERNAME="$YOUR_CONFLUENT_API_KEY"
    -e PLUMBER_RELAY_KAFKA_PASSWORD="$YOUR_CONFLUENT_API_SECRET"
    -e PLUMBER_RELAY_KAFKA_SASL_TYPE="plain"
    batchcorp/plumber plumber relay kafka
```

##### Continuously relay messages from a MQTT topic to a Batch.sh collection

```bash
docker run -d --name plumber-mqtt -p 8080:8080 \
    -e PLUMBER_RELAY_MQTT_ADDRESS=tcp://localhost:1883 \
    -e PLUMBER_RELAY_MQTT_TOPIC=iotdata \
    -e PLUMBER_RELAY_MQTT_QOS=1 \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay mqtt
```

##### Continuously relay messages from a NATS JetStream stream to a Batch.sh collection

```bash
docker run -d --name plumber-natsjs -p 8080:8080 \
    -e PLUMBER_RELAY_NATS_JETSTREAM_DSN=nats://localhost:4222 \
    -e PLUMBER_RELAY_NATS_JETSTREAM_CLIENT_ID=plumber \
    -e PLUMBER_RELAY_NATS_JETSTREAM_STREAM=orders \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber plumber relay mqtt
```


## Change Data Capture

##### Continuously relay Postgres change events to a Batch.sh collection

See documentation at https://docs.batch.sh/event-ingestion/change-data-capture/postgresql for instructions on setting
up PostgreSQL CDC.

##### Continuously relay MongoDB change stream events to a Batch.sh collection

```bash
docker run -d --name plumber-cdc-mongo -p 8080:8080 \
    -e PLUMBER_RELAY_CDCMONGO_DSN=mongodb://mongo.mysite.com:27017 \
    -e PLUMBER_RELAY_CDCMONGO_DATABASE=mydb \
    -e PLUMBER_RELAY_CDCMONGO_COLLECTION=customers \
    -e PLUMBER_RELAY_TOKEN=YOUR_BATCHSH_TOKEN_HERE \
    batchcorp/plumber plumber relay cdc-mongo

```

For more advanced mongo usage, see documentation at https://docs.batch.sh/event-ingestion/change-data-capture/mongodb

## Advanced Usage

#### Decoding protobuf encoded messages and viewing them live

Protobuf is supported for both encode and decode for _all_ backends. There are
three flags that must be specified for protobuf:

1. `--encode-type` for writes or `--decode-type` for reads
2. `--protobuf-dirs` pointing to a directory that contains your `.proto` files
3. `--protobuf-root-message` which indicates what type plumber should attempt to encode/decode output/input

NOTE: `--protobuf-root-message` must specify the _FULL_ path to the type. Ie. 
`events.MyType` (`MyType` is not enough!).

```bash
$ plumber read rabbit --address="amqp://localhost" --exchange events --binding-key \# \
  --decode-type protobuf \
  --protobuf-dirs ~/schemas \ 
  --protobuf-root-message pkg.Message \
  --continuous
  
1: {"some-attribute": 123, "numbers" : [1, 2, 3]}
2: {"some-attribute": 424, "numbers" : [325]}
3: {"some-attribute": 49, "numbers" : [958, 288, 289, 290]}
4: ERROR: Cannot decode message as protobuf "Message"
5: {"some-attribute": 394, "numbers" : [4, 5, 6, 7, 8]}
^C
```

#### Writing protobuf messages with source jsonpb

NOTE: "jsonpb" is just a JSON representation of your protobuf event. When you
use it as the `--input-type`, `plumber` will read the JSON blob and attempt
to decode it _into_ your specified root message, followed by writing the []byte
slice to the message bus.

```bash
$ plumber write rabbit --exchange events --routing-key foo.bar  \
  --protobuf-dirs ~/schemas \
  --protobuf-root-message pkg.Message \
  --input-file ~/fakes/some-jsonpb-file.json \ 
  --encode-type jsonpb
```

#### Shallow envelope protobuf messages

Plumber supports ["shallow envelope"](https://www.confluent.io/blog/spring-kafka-protobuf-part-1-event-data-modeling/#shallow-envelope) protobuf messages consisting of one type of protobuf message used to decode
the message itself, and another type of message used to decode the protobuf contents of a payload field inside the envelope. The payload field must be of `bytes` type.

To read/write shallow envelope messages with plumber, you will need to specify the following additional flags:

1. `--protobuf-envelope-type shallow` - To indicate that the message is a shallow envelope
2. `--shallow-envelope-field-number` - Protobuf field number of the envelope's field which contains the encoded protobuf data
3. `--shallow-envelope-message` - The protobuf message name used to encode the data in the field


Example protobuf we will read/write with:

```protobuf
syntax = "proto3";

package shallow;

// Represents a shallow envelope
message Envelope {
  string id = 1;
  bytes data = 2;
}

// Message is what goes into Envelope's Data field
message Payload {
  string name = 1;
}
```
##### Writing shallow envelope

```bash
plumber write kafka --topics testing \
  --protobuf-dirs test-assets/shallow-envelope/ \
  --protobuf-root-message shallow.Envelope \
  --input-file test-assets/shallow-envelope/example-payload.json \
  --protobuf-envelope-type shallow \
  --shallow-envelope-message shallow.Payload \
  --shallow-envelope-field-number=2 \
  --encode-type jsonpb
```

#### Reading shallow envelope

```bash
plumber read kafka --topics testing \
  --protobuf-dirs test-assets/shallow-envelope/ \
  --protobuf-root-message shallow.Envelope \
  --protobuf-envelope-type shallow \
  --shallow-envelope-message shallow.Payload \
  --shallow-envelope-field-number=2 \
  --decode-type protobuf
```

### Using File Descriptor Sets

Plumber supports using protobuf file descriptor set files for decoding and encoding protobuf messages, instead of
using a directory of `.proto` files. This method is more reliable than using `--protobuf-dirs` flag as it ensures
that there won't be any include path issues.

For help with generating an `.fds` file from your `.proto` files, see https://docs.batch.sh/platform/components/what-are-schemas#protocol-buffers

#### Writing using FDS

```bash
plumber write kafka --topics fdstest1 \
  --protobuf-descriptor-set test-assets/protobuf-any/sample/protos.fds \
  --protobuf-root-message sample.Envelope \
  --encode-type jsonpb \
  --input-file test-assets/protobuf-any/payload.json
```

#### Reading using FDS

```bash
plumber read kafka --topics fdstest1 \
  --protobuf-descriptor-set test-assets/protobuf-any/sample/protos.fds \
  --protobuf-root-message sample.Envelope \
  --decode-type protobuf
```

#### Using Avro schemas when reading or writing
```bash
$ plumber write kafka --topics=orders --avro-schema-file=some_schema.avsc --input-file=your_data.json
$ plumber read kafka --topics=orders --avro-schema-file=some_schema.avsc
```
