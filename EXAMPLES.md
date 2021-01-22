# Plumber Usage Examples

  * [Consuming](#consuming)
       * [AWS SQS](#aws-sqs)
       * [RabbitMQ](#rabbitmq)
       * [Kafka](#kafka)
       * [Azure Service Bus](#azure-service-bus)
       * [NATS](#nats)
       * [Redis PubSub](#redis-pubsub)
  * [Publishing](#publishing)
       * [AWS SQS](#aws-sqs-1)
       * [AWS SNS](#aws-sns)
       * [RabbitMQ](#rabbitmq-1)
       * [Kafka](#kafka-1)
       * [Azure Service Bus](#azure-service-bus-1)
       * [NATS](#nats-1)
       * [Redis PubSub](#redis-pubsub-1)
  * [Relay Mode](#relay-mode)
       * [Continuously relay messages from your RabbitMQ instance to a Batch.sh collection](#continuously-relay-messages-from-your-rabbitmq-instance-to-a-batchsh-collection)
       * [Continuously relay messages from an SQS queue to a Batch.sh collection](#continuously-relay-messages-from-an-sqs-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure queue to a Batch.sh collection](#continuously-relay-messages-from-an-azure-queue-to-a-batchsh-collection)
       * [Continuously relay messages from an Azure topic to a Batch.sh collection](#continuously-relay-messages-from-an-azure-topic-to-a-batchsh-collection)
       * [Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)](#continuously-relay-messages-from-a-kafka-topic-on-confluent-to-a-batchsh-collection-via-cli)
  * [Advanced Usage](#advanced-usage)
       * [Decoding protobuf encoded messages and viewing them live](#decoding-protobuf-encoded-messages-and-viewing-them-live)

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
plumber read kafka --topic orders --address="some-machine.domain.com:9092" --line-numbers --follow
```

Continuously read messages

```
plumber read kafka --topic orders --address="some-machine.domain.com:9092" --follow
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

##### NATS

```bash
plumber read nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject"
```

##### Redis PubSub

```bash
plumber read redis --address="localhost:6379" --channel="new-orders"
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

##### NATS

```bash
plumber write nats --address="nats://user:pass@nats.test.io:4222" --subject "test-subject" --input-data "Hello World"
```

##### Redis PubSub

```bash
plumber write redis --address="localhost:6379" --channel="new-orders" --input-data="{\"order_id\": \"A-3458-654-1\", \"status\": \"processed\"}"
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

#### Continuously relay messages from a Kafka topic (on Confluent) to a Batch.sh collection (via CLI)

```
export PLUMBER_RELAY_TYPE="kafka"
export PLUMBER_RELAY_TOKEN="$YOUR-BATCHSH-TOKEN-HERE"
export PLUMBER_RELAY_KAFKA_ADDRESS="pkc-4kgmg.us-west-2.aws.confluent.cloud:9092"
export PLUMBER_RELAY_KAFKA_TOPIC="$YOUR_TOPIC"
export PLUMBER_RELAY_KAFKA_INSECURE_TLS="true"
export PLUMBER_RELAY_KAFKA_USERNAME="$YOUR_CONFLUENT_API_KEY"
export PLUMBER_RELAY_KAFKA_PASSWORD="$YOUR_CONFLUENT_API_SECRET"
export PLUMBER_RELAY_KAFKA_SASL_TYPE="plain"

$ plumber relay
```

## Advanced Usage

##### Decoding protobuf encoded messages and viewing them live

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

##### Using Avro schemas when reading or writing
```bash
$ plumber write kafka --topic=orders --avro-schema=some_schema.avsc --input-file=your_data.json
$ plumber read kafka --topic=orders --avro-schema=some_schema.avsc
```

<sub>
If your schemas are located in multiple places, you can specify `--protobuf-dir`
multiple times. Treat it the same as you would `protoc -I`.
</sub> 
