# Plumber Usage Examples

  * [Consuming](#consuming)
       * [AWS SQS](#aws-sqs)
       * [RabbitMQ](#rabbitmq)
       * [Kafka](#kafka)
  * [Publishing](#publishing)
       * [AWS SQS](#aws-sqs-1)
       * [RabbitMQ](#rabbitmq-1)
       * [Kafka](#kafka-1)
  * [Relay Mode](#relay-mode)
       * [Continuously relay messages from your RabbitMQ instance to a Batch.sh collection](#continuously-relay-messages-from-your-rabbitmq-instance-to-a-batchsh-collection)
       * [Continuously relay messages from an SQS queue to a Batch.sh collection](#continuously-relay-messages-from-an-sqs-queue-to-a-batchsh-collection)
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

```
$ docker run -d --name plumber-sqs -p 8080:8080 \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e PLUMBER_RELAY_SQS_QUEUE_NAME=TestQueue \
    -e PLUMBER_RELAY_TYPE=aws-sqs \
    -e PLUMBER_RELAY_TOKEN=$YOUR-BATCHSH-TOKEN-HERE \
    batchcorp/plumber 
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

<sub>
If your schemas are located in multiple places, you can specify `--protobuf-dir`
multiple times. Treat it the same as you would `protoc -I`.
</sub> 