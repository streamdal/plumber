# Plumber Usage Examples

  * [Consuming](#consuming)
       * [AWS SQS](#aws-sqs)
       * [RabbitMQ](#rabbitmq)
       * [Kafka](#kafka)
       * [Azure Service Bus](#azure-service-bus)
       * [Azure Event Hub](#azure-event-hub)
       * [NATS](#nats)
       * [NATS Streaming](#nats-streaming)
       * [Redis PubSub](#redis-pubsub)
       * [Redis Streams](#redis-streams)
       * [GCP Pub/Sub](#gcp-pubsub)
       * [Postgres CDC](#cdc-postgres)
       * [MQTT](#mqtt)
       * [Apache Pulsar](#apache-pulsar)
       * [NSQ](#nsq)
       * [Thrift Decoding](#thrift)
  * [Publishing](#publishing)
       * [AWS SQS](#aws-sqs-1)
       * [AWS SNS](#aws-sns)
       * [RabbitMQ](#rabbitmq-1)
       * [Kafka](#kafka-1)
       * [Azure Service Bus](#azure-service-bus-1)
       * [Azure Event Hub](#azure-event-hub-1)
       * [NATS](#nats-1)
       * [NATS Streaming](#nats-streaming-1)
       * [Redis PubSub](#redis-pubsub-1)
       * [Redis Streams](#redis-streams-1)
       * [GCP Pub/Sub](#gcp-pubsub-1)
       * [MQTT](#mqtt-1)
       * [Apache Pulsar](#apache-pulsar-1)
       * [NSQ](#nsq-1)
  * [Relay Mode](#relay-mode)
       * [Continuously relay messages from your RabbitMQ instance to a Batch.sh collection](#continuously-relay-messages-from-your-rabbitmq-instance-to-a-batchsh-collection)
       * [Continuously relay messages from an SQS queue to a Batch.sh collection](#continuously-relay-messages-from-an-sqs-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure queue to a Batch.sh collection](#continuously-relay-messages-from-an-azure-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure topic to a Batch.sh collection](#continuously-relay-messages-from-an-azure-topic-to-a-batchsh-collection)
       * [Continuously relay messages for multiple Redis channels to a Batch.sh collection](#continuously-relay-messages-from-multiple-redis-channels-to-a-batchsh-collection)
       * [Continuously relay messages for multiple Redis streams to a Batch.sh collection](#continuously-relay-messages-from-multiple-redis-streams-to-a-batchsh-collection)
       * [Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)](#continuously-relay-messages-from-a-kafka-topic-on-confluent-to-a-batchsh-collection-via-cli)
       * [Continuously relay messages from a MQTT topic to a Batch.sh collection](#continuously-relay-messages-from-a-mqtt-topic-to-a-batchsh-collection)
  * [Change Data Capture](#change-data-capture)
       * [Continuously relay Postgres change events to a Batch.sh collection](#continuously-relay-postgres-change-events-to-a-batchsh-collection)
       * [Continuously relay MongoDB change stream events to a Batch.sh collection](#continuously-relay-mongodb-change-stream-events-to-a-batchsh-collection)
  * [Advanced Usage](#advanced-usage)
       * [Decoding protobuf encoded messages and viewing them live](#decoding-protobuf-encoded-messages-and-viewing-them-live)
       * [Prometheus Metrics](#prometheus-metrics)

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
plumber read aws-sqs --queue-name=orders --follow
```

Poll for new messages for X seconds

```
plumber read aws-sqs --queue-name=orders --wait-time-seconds=20
```

##### RabbitMQ

```
plumber read rabbit
    --address="amqp://localhost:5672" \
    --exchange=testex \
    --queue=testqueue \
    --routing-key="orders.#"
    --follow
```

##### Kafka

Read a single message

```
plumber read kafka --topic orders --address="broker1.domain.com:9092"
```

You may specify multiple brokers by specifying the `--address` flag multiple times

```
plumber read kafka --topic orders \
    --address="broker1.domain.com:9092" \
    --address="broker2.domain.com:9092" \
    --address="broker3.domain.com:9092" \
    --follow
```

Continuously read messages

```
plumber read kafka --topic orders --address="broker1.domain.com:9092" --follow
```

##### Azure Service Bus

Reading from a topic

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber read azure --topic="new-orders" --subscription="copy-of-new-orders"
```

Reading from a queue

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber read azure --queue "new-orders"
```

##### Azure Event Hub

Read first available message from any partition

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber read azure-eventhub
```

##### NATS

```bash
plumber read nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject"
```

##### NATS Streaming

```bash
plumber read nats-streaming --address="nats://user:pass@nats.test.io:4222" --channel "orders" --cluster-id "test-cluster" --client-id "plumber"
```

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
plumber read gcp-pubsub --project-id=PROJECT_ID --sub-id=SUBSCRIPTION
```

##### MQTT

```bash
plumber read mqtt --address tcp://localhost:1883 --topic iotdata -qos 1
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

Plumber can decode thrift output, and display it as nested JSON. The key is the field's ID, and the
value is the actual value in the message. Add the `--json` flag for human readable output

```bash
plumber read kafka --topic orders --thrift --json

{
  "1": 54392501,
  "2": "Test Order",
  "3": {
    "1": "Product Name"
    "2": "green",
    "3": "2091.99"
  }
}
```

## Publishing

##### AWS SQS

```
plumber write aws-sqs --queue-name=NewOrders --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### RabbitMQ

```
plumber write rabbit --address="aqmp://rabbit.yourdomain.net:5672" --exchange=NewOrders --routing-key="orders.oregon.coffee" --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Kafka

```
plumber write kafka --address="localhost:9092" --topic=neworders --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

You may specify multiple brokers by specifying the `--address` flag multiple times.

To read from more than one topic, you may specify multiple `--topic` flags.

```
plumber write kafka --topic neworders \
    --address "broker1.domain.com:9092" \
    --address "broker2.domain.com:9092" \
    --address "broker3.domain.com:9092" \
    --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### AWS SNS

```bash
plumber write aws-sns --topic="arn:aws:sns:us-east-2:123456789012:MyTopic" --input-data="A new is ready for processing!"
```

##### Azure Service Bus

Publishing to a topic

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber write azure --topic="new-orders" --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

Publishing to a queue

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://plumbertopictest.servicebus.windows.net/;SharedAccessKeyName=...."

plumber write azure --queue="new-orders" --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Azure Event Hub

Publish to random partition

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber write azure-eventhub --input-data "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}" --message-id "neworder123"
```

Publish to specific partition key

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://plumbertest.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=....=;EntityPath=...""

plumber write azure-eventhub --input-data "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}" --message-id "neworder123" --partition-key "neworders"
```

##### NATS

```bash
plumber write nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject" --input-data "Hello World"
```

##### NATS Streaming

```bash
plumber write nats-streaming --address="nats://user:pass@nats.test.io:4222" --channel "orders" --cluster-id "test-cluster" --client-id "plumber-producer" --input-data "{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Redis PubSub

```bash
plumber write redis-pubsub --address="localhost:6379" --channels="new-orders" --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### Redis Streams

```bash
plumber write redis-streams --address="localhost:6379" --streams="new-orders" --key foo --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### GCP Pub/Sub

```bash
plumber write gcp-pubsub --topic-id=TOPIC --project-id=PROJECT_ID --input-data='{"Sensor":"Room J","Temp":19}' 
```

##### MQTT

```bash
plumber write mqtt --address tcp://localhost:1883 --topic iotdata -qos 1 --input-data "{\"id\": 123, \"temperature\": 15}"
```
##### Apache Pulsar

```bash
plumber write pulsar --topic NEWORDERS --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

##### NSQ

```bash
plumger write nsq --nsqd-address localhost:4050 --topic orders --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
```

## Relay Mode

##### Continuously relay messages from your RabbitMQ instance to a Batch.sh collection

```bash
$ docker run --name plumber-rabbit -p 8080:8080 \
    -e PLUMBER_RELAY_TYPE=rabbit \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    -e PLUMBER_RELAY_RABBIT_EXCHANGE=my_exchange \
    -e PLUMBER_RELAY_RABBIT_QUEUE=my_queue \
    -e PLUMBER_RELAY_RABBIT_ROUTING_KEY=some.routing.key \
    -e PLUMBER_RELAY_RABBIT_QUEUE_EXCLUSIVE=false \
    -e PLUMBER_RELAY_RABBIT_QUEUE_DURABLE=true \
    batchcorp/plumber
```

##### Continuously relay messages from an SQS queue to a Batch.sh collection

```bash
docker run -d --name plumber-sqs -p 8080:8080 \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e PLUMBER_RELAY_SQS_QUEUE_NAME=TestQueue \
    -e PLUMBER_RELAY_TYPE=aws-sqs \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```

##### Continuously relay messages from an Azure queue to a Batch.sh collection

```bash
docker run -d --name plumber-azure -p 8080:8080 \
    -e SERVICEBUS_CONNECTION_STRING="Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName..."
    -e PLUMBER_RELAY_AZURE_QUEUE_NAME=neworders \
    -e PLUMBER_RELAY_TYPE=azure \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```

##### Continuously relay messages from an Azure topic to a Batch.sh collection

```bash
docker run -d --name plumber-azure -p 8080:8080 \
    -e SERVICEBUS_CONNECTION_STRING="Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName..."
    -e PLUMBER_RELAY_AZURE_TOPIC_NAME=neworders \
    -e PLUMBER_RELAY_AZURE_SUBSCRIPTION=some-sub \
    -e PLUMBER_RELAY_TYPE=azure \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```

##### Continuously relay messages from multiple Redis channels to a Batch.sh collection

```bash
docker run -d --name plumber-redis-pubsub -p 8080:8080 \
    -e PLUMBER_RELAY_REDIS_PUBSUB_ADDRESS=localhost:6379 \
    -e PLUMBER_RELAY_REDIS_PUBSUB_CHANNELS=channel1,channel2 \
    -e PLUMBER_RELAY_TYPE=redis-pubsub \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```

##### Continuously relay messages from multiple Redis streams to a Batch.sh collection

```bash
docker run -d --name plumber-redis-streams -p 8080:8080 \
    -e PLUMBER_RELAY_REDIS_STREAMS_ADDRESS=localhost:6379 \
    -e PLUMBER_RELAY_REDIS_STREAMS_STREAMS=stream1,stream2 \
    -e PLUMBER_RELAY_TYPE=redis-streams \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```

##### Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)

```
export PLUMBER_RELAY_TYPE="kafka"
export PLUMBER_RELAY_TOKEN="$YOUR-BATCHSH-TOKEN-HERE"
export PLUMBER_RELAY_KAFKA_ADDRESS="pkc-4kgmg.us-west-2.aws.confluent.cloud:9092,pkc-5kgmg.us-west-2.aws.confluent.cloud:9092"
export PLUMBER_RELAY_KAFKA_TOPIC="$YOUR_TOPIC"
export PLUMBER_RELAY_KAFKA_INSECURE_TLS="true"
export PLUMBER_RELAY_KAFKA_USERNAME="$YOUR_CONFLUENT_API_KEY"
export PLUMBER_RELAY_KAFKA_PASSWORD="$YOUR_CONFLUENT_API_SECRET"
export PLUMBER_RELAY_KAFKA_SASL_TYPE="plain"

$ plumber relay
```

##### Continuously relay messages from a MQTT topic to a Batch.sh collection

```bash
docker run -d --name plumber-mqtt -p 8080:8080 \
    -e PLUMBER_RELAY_MQTT_ADDRESS=tcp://localhost:1883 \
    -e PLUMBER_RELAY_MQTT_TOPIC=iotdata \
    -e PLUMBER_RELAY_MQTT_QOS=1 \
    -e PLUMBER_RELAY_TYPE=mqtt \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
```


## Change Data Capture

##### Continuously relay Postgres change events to a Batch.sh collection

See documentation at https://docs.batch.sh/event-ingestion/change-data-capture/postgresql for instructions on setting
up PostgreSQL CDC

##### Continuously relay Monbodb change stream events to a Batch.sh collection

```bash
docker run -d --name plumber-cdc-mongo -p 8080:8080 \
    -e PLUMBER_RELAY_CDCMONGO_DSN=mongodb://mongo.mysite.com:27017 \
    -e PLUMBER_RELAY_TYPE=cdc-mongo \
    -e PLUMBER_RELAY_CDCMONGO_DATABASE=mydb \
    -e PLUMBER_RELAY_CDCMONGO_COLLECTION=customers \
    -e PLUMBER_RELAY_TOKEN=YOUR_BATCHSH_TOKEN_HERE \
    batchcorp/plumber 

```

For more advanced mongo usage, see documentation at https://docs.batch.sh/event-ingestion/change-data-capture/mongodb

## Advanced Usage

##### Decoding protobuf encoded messages and viewing them live

```bash
$ plumber read rabbit --address="amqp://localhost" --exchange events --routing-key \# \
  --protobuf-dir ~/schemas --protobuf-root-message pkg.Message --follow
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
  --protobuf-dir ~/schemas --protobuf-root-message pkg.Message \
  --input-file ~/fakes/some-jsonpb-file.json --input-type jsonpb
```

##### Using Avro schemas when reading or writing
```bash
$ plumber write kafka --topic=orders --avro-schema=some_schema.avsc --input-file=your_data.json
$ plumber read kafka --topic=orders --avro-schema=some_schema.avsc
```

<sub>
If your schemas are located in multiple places, you can specify `--protobuf-dir`
multiple times. Treat it the same as you would `protoc -I`.
</sub> 

##### Prometheus Metrics

In relay mode, plumber will launch a http server exposing Prometheus metrics running at http://localhost:8080/metrics

Prometheus metrics can be pulled from Plumber by adding a new source to your `prometheus.yml` config

```yaml
scrape_configs:
- job_name: plumber
  scrape_interval: 5s
  static_configs:
  - targets:
    - your-hostname:8080
```

You may modify the listen address/port using the `PLUMBER_RELAY_HTTP_LISTEN_ADDRESS` environment variable or
the `--listen-address` flag.

The following metrics are available in addition to all golang metrics

| Metric | Type | Description |
|----|----|----|
| plumber_relay_rate | gauge | Current rare of messages being relayed to Batch.sh (5 second interval) |
| plumber_relay_total | counter | Total number of events relayed to Batch.sh | 
| plumber_read_errors | counter | Number of errors when reading messages |
| plumber_grpc_errors | counter | Number of errors when making GRPC calls |